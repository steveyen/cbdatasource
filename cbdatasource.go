//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbdatasource

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/gomemcached"
	"github.com/couchbase/gomemcached/client"
	"github.com/couchbaselabs/go-couchbase"
)

// This is the main interface returned by NewBucketDataSource().
type BucketDataSource interface {
	// Use Start() to kickoff connectivity to a Couchbase cluster,
	// after which calls will be made to the Receiver's methods.
	Start() error

	// Force a cluster map refresh.  A reason string of "" is valid.
	Kick(reason string) error

	// Returns an immutable snapshot of stats.
	Stats(dest *BucketDataSourceStats) error

	// Stops the underlying goroutines.
	Close() error
}

// Interface implemented by the application, or the receiver of data.
type Receiver interface {
	// Invoked in advisory fashion by the BucketDataSource when it
	// encounters an error.  The BucketDataSource will continue to try
	// to "heal" and restart connections, etc, as necessary.  The
	// Receiver has a recourse during these error notifications of
	// simply Close()'ing the BucketDataSource.
	OnError(error)

	// Invoked by the BucketDataSource when it has received a mutation
	// from the data source.  Receiver implementation is responsible
	// for making its own copies of the key and request.
	DataUpdate(vbucketId uint16, key []byte, seq uint64,
		r *gomemcached.MCRequest) error

	// Invoked by the BucketDataSource when it has received a deletion
	// or expiration from the data source.  Receiver implementation is
	// responsible for making its own copies of the key and request.
	DataDelete(vbucketId uint16, key []byte, seq uint64,
		r *gomemcached.MCRequest) error

	// An callback invoked by the BucketDataSource when it has
	// received a start snapshot message from the data source.  The
	// Receiver implementation, for example, might choose to optimize
	// persistence perhaps by preparing a batch write to
	// application-specific storage.
	SnapshotStart(vbucketId uint16, snapStart, snapEnd uint64, snapType uint32) error

	// The Receiver should persist the value parameter of
	// SetMetaData() for retrieval during some future call to
	// GetMetaData() by the BucketDataSource.  The metadata value
	// should be considered "in-stream", or as part of the sequence
	// history of mutations.  That is, a later Rollback() to some
	// previous sequence number for a particular vbucketId should
	// rollback both persisted metadata and regular data.
	SetMetaData(vbucketId uint16, value []byte) error

	// GetMetaData() should return the opaque value previously
	// provided by an earlier call to SetMetaData().  If there was no
	// previous call to SetMetaData(), such as in the case of a brand
	// new instance of a Receiver (as opposed to a restarted or
	// reloaded Receiver), the Receiver should return (nil, 0, nil)
	// for (value, lastSeq, err), respectively.  The lastSeq should be
	// the last sequence number received and persisted during calls to
	// the Receiver's DataUpdate() & DataDelete() methods.
	GetMetaData(vbucketId uint16) (value []byte, lastSeq uint64, err error)

	// Invoked by the BucketDataSource when the datasource signals a
	// rollback during stream initialization.  Note that both data and
	// metadata should be rolled back.
	Rollback(vbucketId uint16, rollbackSeq uint64) error
}

type BucketDataSourceOptions struct {
	// Optional - used during UPR_OPEN stream start.  If empty a
	// random name will be automatically generated.
	Name string

	// Factor (like 1.5) to increase sleep time between retries
	// in connecting to a cluster manager node.
	ClusterManagerBackoffFactor float32

	// Initial sleep time (millisecs) before first retry to cluster manager.
	ClusterManagerSleepInitMS int

	// Maximum sleep time (millisecs) between retries to cluster manager.
	ClusterManagerSleepMaxMS int

	// Factor (like 1.5) to increase sleep time between retries
	// in connecting to a data manager node.
	DataManagerBackoffFactor float32

	// Initial sleep time (millisecs) before first retry to data manager.
	DataManagerSleepInitMS int

	// Maximum sleep time (millisecs) between retries to data manager.
	DataManagerSleepMaxMS int

	// Buffer size in bytes provided for UPR flow control.
	FeedBufferSizeBytes uint32

	// Used for UPR flow control and buffer-ack messages when this
	// percentage of FeedBufferSizeBytes is reached.
	FeedBufferAckThreshold float32

	// Optional function to connect to a couchbase cluster manager bucket.
	// Defaults to ConnectBucket() function in this package.
	ConnectBucket func(serverURL, poolName, bucketName, bucketUUID string,
		authFunc AuthFunc) (Bucket, error)

	// Optional function to connect to a couchbase data manager node.
	// Defaults to memcached.Connect().
	Connect func(protocol, dest string) (*memcached.Client, error)
}

type Bucket interface {
	Close()
	GetUUID() string
	VBServerMap() *couchbase.VBucketServerMap
}

var DefaultBucketDataSourceOptions = &BucketDataSourceOptions{
	ClusterManagerBackoffFactor: 1.5,
	ClusterManagerSleepInitMS:   100,
	ClusterManagerSleepMaxMS:    1000,

	DataManagerBackoffFactor: 1.5,
	DataManagerSleepInitMS:   100,
	DataManagerSleepMaxMS:    1000,

	FeedBufferSizeBytes:    20000000, // ~20MB; see UPR_CONTROL/connection_buffer_size.
	FeedBufferAckThreshold: 0.2,
}

// See the BucketDataSource.Stats() method for how to retrieve stat
// metrics from a BucketDataSource.  All the metrics here prefixed
// with "Tot" are monotonic counters: they only increase.
type BucketDataSourceStats struct {
	TotStart  uint64
	TotKick   uint64
	TotKickOk uint64

	TotRefreshCluster                 uint64
	TotRefreshClusterConnectBucket    uint64
	TotRefreshClusterConnectBucketErr uint64
	TotRefreshClusterConnectBucketOk  uint64
	TotRefreshClusterVBMNilErr        uint64
	TotRefreshClusterKickWorkers      uint64
	TotRefreshClusterKickWorkersOk    uint64
	TotRefreshClusterAwokenClosed     uint64
	TotRefreshClusterAwokenStopped    uint64
	TotRefreshClusterAwokenRestart    uint64
	TotRefreshClusterAwoken           uint64
	TotRefreshClusterDone             uint64

	TotRefreshWorkers                uint64
	TotRefreshWorkersVBMNilErr       uint64
	TotRefreshWorkersVBucketIdErr    uint64
	TotRefreshWorkersServerIdxsErr   uint64
	TotRefreshWorkersMasterIdxErr    uint64
	TotRefreshWorkersMasterServerErr uint64
	TotRefreshWorkersRemoveWorker    uint64
	TotRefreshWorkersAddWorker       uint64
	TotRefreshWorkersKickWorker      uint64
	TotRefreshWorkersCloseWorker     uint64
	TotRefreshWorkersDone            uint64

	TotWorkerStart      uint64
	TotWorkerDone       uint64
	TotWorkerBody       uint64
	TotWorkerBodyKick   uint64
	TotWorkerConnect    uint64
	TotWorkerConnectErr uint64
	TotWorkerConnectOk  uint64
	TotWorkerUPROpenErr uint64
	TotWorkerUPROpenOk  uint64

	TotWorkerTransmitStart uint64
	TotWorkerTransmit      uint64
	TotWorkerTransmitErr   uint64
	TotWorkerTransmitOk    uint64
	TotWorkerTransmitDone  uint64

	TotWorkerReceiveStart uint64
	TotWorkerReceive      uint64
	TotWorkerReceiveErr   uint64
	TotWorkerReceiveOk    uint64

	TotWorkerBufferAck     uint64
	TotWorkerSendEndCh     uint64
	TotWorkerRecvCh        uint64
	TotWorkerRecvChDone    uint64
	TotWorkerHandleRecvErr uint64
	TotWorkerHandleRecvOk  uint64

	TotRefreshWorker     uint64
	TotRefreshWorkerDone uint64
	TotRefreshWorkerOk   uint64

	TotUPRDataChange                       uint64
	TotUPRDataChangeMutation               uint64
	TotUPRDataChangeDeletion               uint64
	TotUPRDataChangeExpiration             uint64
	TotUPRDataChangeErr                    uint64
	TotUPRDataChangeOk                     uint64
	TotUPRCloseStream                      uint64
	TotUPRCloseStreamRes                   uint64
	TotUPRCloseStreamResStateErr           uint64
	TotUPRCloseStreamResErr                uint64
	TotUPRCloseStreamResOk                 uint64
	TotUPRStreamReq                        uint64
	TotUPRStreamReqWant                    uint64
	TotUPRStreamReqRes                     uint64
	TotUPRStreamReqResStateErr             uint64
	TotUPRStreamReqResFail                 uint64
	TotUPRStreamReqResRollback             uint64
	TotUPRStreamReqResRollbackErr          uint64
	TotUPRStreamReqResWantAfterRollbackErr uint64
	TotUPRStreamReqResKick                 uint64
	TotUPRStreamReqResSuccess              uint64
	TotUPRStreamReqResSuccessOk            uint64
	TotUPRStreamReqResFLogErr              uint64
	TotUPRStreamEnd                        uint64
	TotUPRStreamEndStateErr                uint64
	TotUPRStreamEndKick                    uint64
	TotUPRSnapshot                         uint64
	TotUPRSnapshotStateErr                 uint64
	TotUPRSnapshotStart                    uint64
	TotUPRSnapshotStartErr                 uint64
	TotUPRSnapshotOk                       uint64
	TotUPRNoop                             uint64
	TotUPRControl                          uint64
	TotUPRControlErr                       uint64
	TotUPRBufferAck                        uint64
	TotUPRBufferAckErr                     uint64

	TotWantCloseRequestedVBucketErr uint64
	TotWantClosingVBucketErr        uint64

	TotGetVBucketMetaData             uint64
	TotGetVBucketMetaDataUnmarshalErr uint64
	TotGetVBucketMetaDataErr          uint64
	TotGetVBucketMetaDataOk           uint64

	TotSetVBucketMetaData           uint64
	TotSetVBucketMetaDataMarshalErr uint64
	TotSetVBucketMetaDataErr        uint64
	TotSetVBucketMetaDataOk         uint64
}

type AuthFunc func(kind string, challenge []byte) (response []byte, err error)

// --------------------------------------------------------

// This internal struct is exposed to enable json marshaling.
type VBucketMetaData struct {
	SeqStart    uint64     `json:"seqStart"`
	SeqEnd      uint64     `json:"seqEnd"`
	SnapStart   uint64     `json:"snapStart"`
	SnapEnd     uint64     `json:"snapEnd"`
	FailOverLog [][]uint64 `json:"failOverLog"`
}

type bucketDataSource struct {
	serverURLs []string
	poolName   string
	bucketName string
	bucketUUID string
	vbucketIds []uint16
	authFunc   AuthFunc
	receiver   Receiver
	options    *BucketDataSourceOptions

	refreshClusterCh chan string
	refreshWorkersCh chan string
	closedCh         chan bool

	stats BucketDataSourceStats

	m    sync.Mutex // Protects all the below fields.
	life string     // Valid life states: "" (unstarted); "running"; "closed".
	vbm  *couchbase.VBucketServerMap
}

// The application must supply an array of 1 or more serverURLs (or
// "seed" URL's) to Couchbase Server cluster-manager REST URL
// endpoints, like "http://localhost:8091".  The BucketDataSource
// (after Start()'ing) will try each serverURL, in turn, until it can
// get a successful cluster map.  Additionally, the application must
// supply a poolName & bucketName from where the BucketDataSource will
// retrieve data.  The optional bucketUUID is double-checked by the
// BucketDataSource to ensure we have the correct bucket, and a
// bucketUUID of "" means skip the bucketUUID validation.  An optional
// array of vbucketId numbers allows the application to specify which
// vbuckets to retrieve; and the vbucketIds array can be nil which
// means all vbuckets are retrieved by the BucketDataSource.  The
// application must supply its own implementation of the Receiver
// interface (see the example program as a sample).  The optional
// options parameter (which may be nil) allows the application to
// specify advanced parameters like backoff and retry-sleep values.
func NewBucketDataSource(serverURLs []string,
	poolName, bucketName, bucketUUID string,
	vbucketIds []uint16, authFunc AuthFunc,
	receiver Receiver, options *BucketDataSourceOptions) (BucketDataSource, error) {
	if len(serverURLs) < 1 {
		return nil, fmt.Errorf("missing at least 1 serverURL")
	}
	if poolName == "" {
		return nil, fmt.Errorf("missing poolName")
	}
	if bucketName == "" {
		return nil, fmt.Errorf("missing bucketName")
	}
	if receiver == nil {
		return nil, fmt.Errorf("missing receiver")
	}
	if options == nil {
		options = DefaultBucketDataSourceOptions
	}
	return &bucketDataSource{
		serverURLs: serverURLs,
		poolName:   poolName,
		bucketName: bucketName,
		bucketUUID: bucketUUID,
		vbucketIds: vbucketIds,
		authFunc:   authFunc,
		receiver:   receiver,
		options:    options,

		refreshClusterCh: make(chan string, 1),
		refreshWorkersCh: make(chan string, 1),
		closedCh:         make(chan bool),
	}, nil
}

func (d *bucketDataSource) Start() error {
	atomic.AddUint64(&d.stats.TotStart, 1)

	d.m.Lock()
	if d.life != "" {
		d.m.Unlock()
		return fmt.Errorf("Start() called in wrong state: %s", d.life)
	}
	d.life = "running"
	d.m.Unlock()

	backoffFactor := d.options.ClusterManagerBackoffFactor
	if backoffFactor <= 0.0 {
		backoffFactor = DefaultBucketDataSourceOptions.ClusterManagerBackoffFactor
	}
	sleepInitMS := d.options.ClusterManagerSleepInitMS
	if sleepInitMS <= 0 {
		sleepInitMS = DefaultBucketDataSourceOptions.ClusterManagerSleepInitMS
	}
	sleepMaxMS := d.options.ClusterManagerSleepMaxMS
	if sleepMaxMS <= 0 {
		sleepMaxMS = DefaultBucketDataSourceOptions.ClusterManagerSleepMaxMS
	}

	go func() {
		ExponentialBackoffLoop("cbdatasource.refreshCluster",
			func() int { return d.refreshCluster() },
			sleepInitMS, backoffFactor, sleepMaxMS)

		// We reach here when we need to shutdown.
		close(d.refreshWorkersCh)
		atomic.AddUint64(&d.stats.TotRefreshClusterDone, 1)
	}()

	go d.refreshWorkers()

	return nil
}

func (d *bucketDataSource) isRunning() bool {
	d.m.Lock()
	life := d.life
	d.m.Unlock()
	return life == "running"
}

func (d *bucketDataSource) refreshCluster() int {
	atomic.AddUint64(&d.stats.TotRefreshCluster, 1)

	if !d.isRunning() {
		return -1
	}

	for _, serverURL := range d.serverURLs {
		atomic.AddUint64(&d.stats.TotRefreshClusterConnectBucket, 1)

		connectBucket := d.options.ConnectBucket
		if connectBucket == nil {
			connectBucket = ConnectBucket
		}

		bucket, err := connectBucket(serverURL,
			d.poolName, d.bucketName, d.bucketUUID, d.authFunc)
		if err != nil {
			atomic.AddUint64(&d.stats.TotRefreshClusterConnectBucketErr, 1)
			d.receiver.OnError(err)
			continue // Try another serverURL.
		}
		atomic.AddUint64(&d.stats.TotRefreshClusterConnectBucketOk, 1)

		vbm := bucket.VBServerMap()
		if vbm == nil {
			bucket.Close()
			atomic.AddUint64(&d.stats.TotRefreshClusterVBMNilErr, 1)
			d.receiver.OnError(fmt.Errorf("refreshCluster got no vbm,"+
				" serverURL: %s, bucketName: %s, bucketUUID: %s, bucket.UUID: %s",
				serverURL, d.bucketName, d.bucketUUID, bucket.GetUUID()))
			continue // Try another serverURL.
		}

		bucket.Close()

		d.m.Lock()
		d.vbm = vbm
		d.m.Unlock()

		for {
			atomic.AddUint64(&d.stats.TotRefreshClusterKickWorkers, 1)
			d.refreshWorkersCh <- "new-vbm" // Kick the workers to refresh.
			atomic.AddUint64(&d.stats.TotRefreshClusterKickWorkersOk, 1)

			reason, alive := <-d.refreshClusterCh // Wait for a refresh cluster kick.
			if !alive || reason == "CLOSE" {      // Or, if we're closed then shutdown.
				atomic.AddUint64(&d.stats.TotRefreshClusterAwokenClosed, 1)
				return -1
			}
			if !d.isRunning() {
				atomic.AddUint64(&d.stats.TotRefreshClusterAwokenStopped, 1)
				return -1
			}

			// If it's only that a new worker appeared, then we can
			// keep with this inner loop and not have to restart all
			// the way at the top / retrieve a new cluster map, etc.
			if reason != "new-worker" {
				atomic.AddUint64(&d.stats.TotRefreshClusterAwokenRestart, 1)
				return 1 // Assume progress, so restart at first serverURL.
			}

			atomic.AddUint64(&d.stats.TotRefreshClusterAwoken, 1)
		}
	}

	return 0 // Ran through all the serverURLs, so no progress.
}

func (d *bucketDataSource) refreshWorkers() {
	// Keyed by server, value is chan of array of vbucketId's that the
	// worker needs to provide.
	workers := make(map[string]chan []uint16)

	for _ = range d.refreshWorkersCh { // Wait for a refresh kick.
		atomic.AddUint64(&d.stats.TotRefreshWorkers, 1)

		d.m.Lock()
		vbm := d.vbm
		d.m.Unlock()

		if vbm == nil {
			atomic.AddUint64(&d.stats.TotRefreshWorkersVBMNilErr, 1)
			continue
		}

		// If nil vbucketIds, then default to all vbucketIds.
		vbucketIds := d.vbucketIds
		if vbucketIds == nil {
			vbucketIds = make([]uint16, len(vbm.VBucketMap))
			for i := 0; i < len(vbucketIds); i++ {
				vbucketIds[i] = uint16(i)
			}
		}

		// Group the wanted vbucketIds by server.
		vbucketIdsByServer := make(map[string][]uint16)

		for _, vbucketId := range vbucketIds {
			if int(vbucketId) >= len(vbm.VBucketMap) {
				atomic.AddUint64(&d.stats.TotRefreshWorkersVBucketIdErr, 1)
				d.receiver.OnError(fmt.Errorf("refreshWorkers"+
					" saw bad vbucketId: %d, vbm: %#v",
					vbucketId, vbm))
				continue
			}
			serverIdxs := vbm.VBucketMap[vbucketId]
			if serverIdxs == nil || len(serverIdxs) <= 0 {
				atomic.AddUint64(&d.stats.TotRefreshWorkersServerIdxsErr, 1)
				d.receiver.OnError(fmt.Errorf("refreshWorkers"+
					" no serverIdxs for vbucketId: %d, vbm: %#v",
					vbucketId, vbm))
				continue
			}
			masterIdx := serverIdxs[0]
			if int(masterIdx) >= len(vbm.ServerList) {
				atomic.AddUint64(&d.stats.TotRefreshWorkersMasterIdxErr, 1)
				d.receiver.OnError(fmt.Errorf("refreshWorkers"+
					" no masterIdx for vbucketId: %d, vbm: %#v",
					vbucketId, vbm))
				continue
			}
			masterServer := vbm.ServerList[masterIdx]
			if masterServer == "" {
				atomic.AddUint64(&d.stats.TotRefreshWorkersMasterServerErr, 1)
				d.receiver.OnError(fmt.Errorf("refreshWorkers"+
					" no masterServer for vbucketId: %d, vbm: %#v",
					vbucketId, vbm))
				continue
			}
			v, exists := vbucketIdsByServer[masterServer]
			if !exists || v == nil {
				v = []uint16{}
			}
			vbucketIdsByServer[masterServer] = append(v, vbucketId)
		}

		// Remove any extraneous workers.
		for server, workerCh := range workers {
			if _, exists := vbucketIdsByServer[server]; !exists {
				atomic.AddUint64(&d.stats.TotRefreshWorkersRemoveWorker, 1)
				delete(workers, server)
				close(workerCh)
			}
		}

		// Add any missing workers and update workers with their
		// latest vbucketIds.
		for server, serverVBucketIds := range vbucketIdsByServer {
			workerCh, exists := workers[server]
			if !exists || workerCh == nil {
				atomic.AddUint64(&d.stats.TotRefreshWorkersAddWorker, 1)
				workerCh = make(chan []uint16, 1)
				workers[server] = workerCh
				d.workerStart(server, workerCh)
			}

			workerCh <- serverVBucketIds
			atomic.AddUint64(&d.stats.TotRefreshWorkersKickWorker, 1)
		}
	}

	// We reach here when we need to shutdown.
	for _, workerCh := range workers {
		atomic.AddUint64(&d.stats.TotRefreshWorkersCloseWorker, 1)
		close(workerCh)
	}

	close(d.closedCh)
	atomic.AddUint64(&d.stats.TotRefreshWorkersDone, 1)
}

// A worker connects to one data manager server.
func (d *bucketDataSource) workerStart(server string, workerCh chan []uint16) {
	backoffFactor := d.options.DataManagerBackoffFactor
	if backoffFactor <= 0.0 {
		backoffFactor = DefaultBucketDataSourceOptions.DataManagerBackoffFactor
	}
	sleepInitMS := d.options.DataManagerSleepInitMS
	if sleepInitMS <= 0 {
		sleepInitMS = DefaultBucketDataSourceOptions.DataManagerSleepInitMS
	}
	sleepMaxMS := d.options.DataManagerSleepMaxMS
	if sleepMaxMS <= 0 {
		sleepMaxMS = DefaultBucketDataSourceOptions.DataManagerSleepMaxMS
	}

	go func() {
		atomic.AddUint64(&d.stats.TotWorkerStart, 1)

		ExponentialBackoffLoop("cbdatasource.worker-"+server,
			func() int { return d.worker(server, workerCh) },
			sleepInitMS, backoffFactor, sleepMaxMS)

		atomic.AddUint64(&d.stats.TotWorkerDone, 1)
	}()
}

func (d *bucketDataSource) worker(server string, workerCh chan []uint16) int {
	atomic.AddUint64(&d.stats.TotWorkerBody, 1)

	if !d.isRunning() {
		return -1
	}

	atomic.AddUint64(&d.stats.TotWorkerConnect, 1)
	connect := d.options.Connect
	if connect == nil {
		connect = memcached.Connect
	}

	client, err := connect("tcp", server)
	if err != nil {
		atomic.AddUint64(&d.stats.TotWorkerConnectErr, 1)
		d.receiver.OnError(fmt.Errorf("worker connect, server: %s, err: %v",
			server, err))
		return 0
	}
	defer client.Close()
	atomic.AddUint64(&d.stats.TotWorkerConnectOk, 1)

	// TODO: Call client.Auth(user, pswd).

	uprOpenName := d.options.Name
	if uprOpenName == "" {
		uprOpenName = fmt.Sprintf("cbdatasource-%x", rand.Int63())
	}

	err = UPROpen(client, uprOpenName, d.options.FeedBufferSizeBytes)
	if err != nil {
		atomic.AddUint64(&d.stats.TotWorkerUPROpenErr, 1)
		d.receiver.OnError(err)
		return 0
	}
	atomic.AddUint64(&d.stats.TotWorkerUPROpenOk, 1)

	ackBytes :=
		uint32(d.options.FeedBufferAckThreshold * float32(d.options.FeedBufferSizeBytes))

	sendEndCh := make(chan bool)
	sendCh := make(chan *gomemcached.MCRequest)
	recvCh := make(chan *gomemcached.MCResponse)

	cleanup := func(progress int, err error) int {
		if err != nil {
			d.receiver.OnError(err)
		}
		go func() {
			close(sendCh)
			for _ = range recvCh {
			}
		}()
		return progress
	}

	go func() { // Sender goroutine.
		defer close(sendEndCh)

		atomic.AddUint64(&d.stats.TotWorkerTransmitStart, 1)
		for msg := range sendCh {
			atomic.AddUint64(&d.stats.TotWorkerTransmit, 1)
			err := client.Transmit(msg)
			if err != nil {
				atomic.AddUint64(&d.stats.TotWorkerTransmitErr, 1)
				d.receiver.OnError(fmt.Errorf("client.Transmit, err: %v", err))
				return
			}
			atomic.AddUint64(&d.stats.TotWorkerTransmitOk, 1)
		}
		atomic.AddUint64(&d.stats.TotWorkerTransmitDone, 1)
	}()

	go func() { // Receiver goroutine.
		defer close(recvCh)

		atomic.AddUint64(&d.stats.TotWorkerReceiveStart, 1)

		var hdr [gomemcached.HDR_LEN]byte
		var pkt gomemcached.MCRequest

		conn := client.Hijack()

		for {
			// TODO: memory allocation here.
			atomic.AddUint64(&d.stats.TotWorkerReceive, 1)
			_, err := pkt.Receive(conn, hdr[:])
			if err != nil {
				atomic.AddUint64(&d.stats.TotWorkerReceiveErr, 1)
				d.receiver.OnError(fmt.Errorf("pkt.Receive, err: %v", err))
				return
			}
			atomic.AddUint64(&d.stats.TotWorkerReceiveOk, 1)

			if pkt.Opcode == gomemcached.UPR_MUTATION ||
				pkt.Opcode == gomemcached.UPR_DELETION ||
				pkt.Opcode == gomemcached.UPR_EXPIRATION {
				atomic.AddUint64(&d.stats.TotUPRDataChange, 1)

				vbucketId := pkt.VBucket

				seq := binary.BigEndian.Uint64(pkt.Extras[:8])

				if pkt.Opcode == gomemcached.UPR_MUTATION {
					atomic.AddUint64(&d.stats.TotUPRDataChangeMutation, 1)
					err = d.receiver.DataUpdate(vbucketId, pkt.Key, seq, &pkt)
				} else {
					if pkt.Opcode == gomemcached.UPR_DELETION {
						atomic.AddUint64(&d.stats.TotUPRDataChangeDeletion, 1)
					} else {
						atomic.AddUint64(&d.stats.TotUPRDataChangeExpiration, 1)
					}
					err = d.receiver.DataDelete(vbucketId, pkt.Key, seq, &pkt)
				}

				if err != nil {
					atomic.AddUint64(&d.stats.TotUPRDataChangeErr, 1)
					d.receiver.OnError(fmt.Errorf("DataChange, err: %v", err))
					return
				}

				atomic.AddUint64(&d.stats.TotUPRDataChangeOk, 1)
			} else {
				recvCh <- &gomemcached.MCResponse{
					Opcode: pkt.Opcode,
					Cas:    pkt.Cas,
					Opaque: pkt.Opaque,
					Status: gomemcached.Status(pkt.VBucket),
					Extras: pkt.Extras,
					Key:    pkt.Key,
					Body:   pkt.Body,
				}
			}
		}
	}()

	// Values for vbucketId state in currVBucketIds:
	// "" (dead/closed/unknown), "requested", "running", "closing".
	currVBucketIds := map[uint16]string{}

	// Track received bytes in case we need to buffer-ack.
	recvBytesTotal := uint32(0)

	atomic.AddUint64(&d.stats.TotWorkerBodyKick, 1)
	d.Kick("new-worker")

	for {
		select {
		case <-sendEndCh:
			atomic.AddUint64(&d.stats.TotWorkerSendEndCh, 1)
			return cleanup(1, nil) // We saw disconnect; assume we made progress.

		case res, alive := <-recvCh:
			atomic.AddUint64(&d.stats.TotWorkerRecvCh, 1)

			if !alive {
				// If we lost a connection, then maybe a node was rebalanced out,
				// or failed over, so ask for a cluster refresh just in case.
				d.Kick("recvChDone")

				atomic.AddUint64(&d.stats.TotWorkerRecvChDone, 1)
				return cleanup(1, nil) // We saw disconnect; assume we made progress.
			}

			progress, err := d.handleRecv(sendCh, currVBucketIds, res)
			if err != nil {
				atomic.AddUint64(&d.stats.TotWorkerHandleRecvErr, 1)
				return cleanup(progress, err)
			}
			atomic.AddUint64(&d.stats.TotWorkerHandleRecvOk, 1)

			recvBytesTotal +=
				uint32(gomemcached.HDR_LEN) +
					uint32(len(res.Key)+len(res.Extras)+len(res.Body))
			if ackBytes > 0 && recvBytesTotal > ackBytes {
				ack := &gomemcached.MCRequest{Opcode: gomemcached.UPR_BUFFERACK}
				ack.Extras = make([]byte, 4) // TODO: Memory mgmt.
				binary.BigEndian.PutUint32(ack.Extras, uint32(recvBytesTotal))
				atomic.AddUint64(&d.stats.TotWorkerBufferAck, 1)
				sendCh <- ack
				recvBytesTotal = 0
			}

		case wantVBucketIds, alive := <-workerCh:
			atomic.AddUint64(&d.stats.TotRefreshWorker, 1)

			if !alive {
				atomic.AddUint64(&d.stats.TotRefreshWorkerDone, 1)
				return cleanup(-1, nil) // We've been asked to shutdown.
			}

			progress, err :=
				d.refreshWorker(sendCh, currVBucketIds, wantVBucketIds)
			if err != nil {
				return cleanup(progress, err)
			}

			atomic.AddUint64(&d.stats.TotRefreshWorkerOk, 1)
		}
	}

	return cleanup(-1, nil) // Unreached.
}

func (d *bucketDataSource) refreshWorker(sendCh chan *gomemcached.MCRequest,
	currVBucketIds map[uint16]string, wantVBucketIdsArr []uint16) (
	progress int, err error) {
	// Convert to map for faster lookup.
	wantVBucketIds := map[uint16]bool{}
	for _, wantVBucketId := range wantVBucketIdsArr {
		wantVBucketIds[wantVBucketId] = true
	}

	for currVBucketId, state := range currVBucketIds {
		if !wantVBucketIds[currVBucketId] {
			if state == "requested" {
				// A UPR_STREAMREQ request is already on the wire, so
				// error rather than have complex compensation logic.
				atomic.AddUint64(&d.stats.TotWantCloseRequestedVBucketErr, 1)
				return 0, fmt.Errorf("want close requested vbucketId: %d", currVBucketId)
			}
			if state == "running" {
				currVBucketIds[currVBucketId] = "closing"
				atomic.AddUint64(&d.stats.TotUPRCloseStream, 1)
				sendCh <- &gomemcached.MCRequest{
					Opcode:  gomemcached.UPR_CLOSESTREAM,
					VBucket: currVBucketId,
					Opaque:  uint32(currVBucketId),
				}
			} // Else, state of "" or "closing", so no-op.
		}
	}

	for wantVBucketId, _ := range wantVBucketIds {
		state := currVBucketIds[wantVBucketId]
		if state == "closing" {
			// A UPR_CLOSESTREAM request is already on the wire, so
			// error rather than have complex compensation logic.
			atomic.AddUint64(&d.stats.TotWantClosingVBucketErr, 1)
			return 0, fmt.Errorf("want closing vbucketId: %d", wantVBucketId)
		}
		if state == "" {
			currVBucketIds[wantVBucketId] = "requested"
			atomic.AddUint64(&d.stats.TotUPRStreamReqWant, 1)
			err := d.sendStreamReq(sendCh, wantVBucketId)
			if err != nil {
				return 0, err
			}
		} // Else, state of "requested" or "running", so no-op.
	}

	return 0, nil
}

func (d *bucketDataSource) handleRecv(sendCh chan *gomemcached.MCRequest,
	currVBucketIds map[uint16]string, res *gomemcached.MCResponse) (
	progress int, err error) {
	switch res.Opcode {
	case gomemcached.UPR_NOOP:
		atomic.AddUint64(&d.stats.TotUPRNoop, 1)
		sendCh <- &gomemcached.MCRequest{
			Opcode: gomemcached.UPR_NOOP,
			Opaque: res.Opaque,
		}

	case gomemcached.UPR_STREAMREQ:
		atomic.AddUint64(&d.stats.TotUPRStreamReqRes, 1)

		vbucketId := uint16(res.Opaque)
		vbucketIdState := currVBucketIds[vbucketId]

		delete(currVBucketIds, vbucketId)

		if vbucketIdState != "requested" {
			atomic.AddUint64(&d.stats.TotUPRStreamReqResStateErr, 1)
			return 0, fmt.Errorf("streamreq non-requested,"+
				" vbucketId: %d, vbucketIdState: %s, res: %#v",
				vbucketId, vbucketIdState, res)
		}

		if res.Status != gomemcached.SUCCESS {
			atomic.AddUint64(&d.stats.TotUPRStreamReqResFail, 1)

			if res.Status == gomemcached.ROLLBACK {
				atomic.AddUint64(&d.stats.TotUPRStreamReqResRollback, 1)

				if len(res.Extras) != 8 {
					return 0, fmt.Errorf("bad rollback extras: %#v", res)
				}

				rollbackSeq := binary.BigEndian.Uint64(res.Extras)
				err := d.receiver.Rollback(vbucketId, rollbackSeq)
				if err != nil {
					atomic.AddUint64(&d.stats.TotUPRStreamReqResRollbackErr, 1)
					return 0, err
				}

				currVBucketIds[vbucketId] = "requested"
				atomic.AddUint64(&d.stats.TotUPRStreamReqResWantAfterRollbackErr, 1)
				err = d.sendStreamReq(sendCh, vbucketId)
				if err != nil {
					return 0, err
				}
			} else {
				// Maybe the vbucket moved, so kick off a cluster refresh.
				atomic.AddUint64(&d.stats.TotUPRStreamReqResKick, 1)
				d.Kick("stream-req-error")
			}
		} else { // SUCCESS case.
			atomic.AddUint64(&d.stats.TotUPRStreamReqResSuccess, 1)

			flog, err := ParseFailOverLog(res.Body[:])
			if err != nil {
				atomic.AddUint64(&d.stats.TotUPRStreamReqResFLogErr, 1)
				return 0, err
			}
			v, _, err := d.getVBucketMetaData(vbucketId)
			if err != nil {
				return 0, err
			}

			v.FailOverLog = flog

			err = d.setVBucketMetaData(vbucketId, v)
			if err != nil {
				return 0, err
			}

			currVBucketIds[vbucketId] = "running"
			atomic.AddUint64(&d.stats.TotUPRStreamReqResSuccessOk, 1)
		}

	case gomemcached.UPR_STREAMEND:
		atomic.AddUint64(&d.stats.TotUPRStreamEnd, 1)

		vbucketId := uint16(res.Status)
		vbucketIdState := currVBucketIds[vbucketId]

		delete(currVBucketIds, vbucketId)

		if vbucketIdState != "running" &&
			vbucketIdState != "closing" {
			atomic.AddUint64(&d.stats.TotUPRStreamEndStateErr, 1)
			return 0, fmt.Errorf("stream-end bad state,"+
				" vbucketId: %d, vbucketIdState: %s, res: %#v",
				vbucketId, vbucketIdState, res)
		}

		// We should not normally see a stream-end, unless we
		// were trying to close.  Maybe the vbucket moved, so
		// kick off a cluster refresh.
		if vbucketIdState != "closing" {
			atomic.AddUint64(&d.stats.TotUPRStreamEndKick, 1)
			d.Kick("stream-end")
		}

	case gomemcached.UPR_CLOSESTREAM:
		atomic.AddUint64(&d.stats.TotUPRCloseStreamRes, 1)

		vbucketId := uint16(res.Opaque)
		vbucketIdState := currVBucketIds[vbucketId]

		if vbucketIdState != "closing" {
			atomic.AddUint64(&d.stats.TotUPRCloseStreamResStateErr, 1)
			return 0, fmt.Errorf("close-stream bad state,"+
				" vbucketId: %d, vbucketIdState: %s, res: %#v",
				vbucketId, vbucketIdState, res)
		}

		if res.Status != gomemcached.SUCCESS {
			atomic.AddUint64(&d.stats.TotUPRCloseStreamResErr, 1)
			return 0, fmt.Errorf("close-stream failed,"+
				" vbucketId: %d, vbucketIdState: %s, res: %#v",
				vbucketId, vbucketIdState, res)
		}

		// At this point, we can ignore this success response to our
		// close-stream request, as the server will send a stream-end
		// afterwards.
		atomic.AddUint64(&d.stats.TotUPRCloseStreamResOk, 1)

	case gomemcached.UPR_SNAPSHOT:
		atomic.AddUint64(&d.stats.TotUPRSnapshot, 1)

		vbucketId := uint16(res.Status)
		vbucketIdState := currVBucketIds[vbucketId]

		if vbucketIdState != "running" {
			atomic.AddUint64(&d.stats.TotUPRSnapshotStateErr, 1)
			return 0, fmt.Errorf("snapshot non-running,"+
				" vbucketId: %d, vbucketIdState: %s, res: %#v",
				vbucketId, vbucketIdState, res)
		}

		if len(res.Extras) < 20 {
			return 0, fmt.Errorf("bad snapshot extras, res: %#v", res)
		}

		v, _, err := d.getVBucketMetaData(vbucketId)
		if err != nil {
			return 0, err
		}

		v.SnapStart = binary.BigEndian.Uint64(res.Extras[0:8])
		v.SnapEnd = binary.BigEndian.Uint64(res.Extras[8:16])

		err = d.setVBucketMetaData(vbucketId, v)
		if err != nil {
			return 0, err
		}

		snapType := binary.BigEndian.Uint32(res.Extras[16:20])

		atomic.AddUint64(&d.stats.TotUPRSnapshotStart, 1)
		err = d.receiver.SnapshotStart(vbucketId,
			v.SnapStart, v.SnapEnd, snapType)
		if err != nil {
			atomic.AddUint64(&d.stats.TotUPRSnapshotStartErr, 1)
			return 0, err
		}

		atomic.AddUint64(&d.stats.TotUPRSnapshotOk, 1)

		// TODO: Do we need to handle snapAck flag in snapType?

	case gomemcached.UPR_CONTROL:
		atomic.AddUint64(&d.stats.TotUPRControl, 1)
		if res.Status != gomemcached.SUCCESS {
			atomic.AddUint64(&d.stats.TotUPRControlErr, 1)
			return 0, fmt.Errorf("failed control: %#v", res)
		}

	case gomemcached.UPR_BUFFERACK:
		atomic.AddUint64(&d.stats.TotUPRBufferAck, 1)
		if res.Status != gomemcached.SUCCESS {
			atomic.AddUint64(&d.stats.TotUPRBufferAckErr, 1)
			return 0, fmt.Errorf("failed bufferack: %#v", res)
		}

	case gomemcached.UPR_OPEN:
		// Opening was long ago, so we should not see UPR_OPEN responses.
		return 0, fmt.Errorf("unexpected upr_open, res: %#v", res)

	case gomemcached.UPR_ADDSTREAM:
		// This normally comes from ns-server / dcp-migrator.
		return 0, fmt.Errorf("unexpected upr_addstream, res: %#v", res)

	case gomemcached.UPR_MUTATION, gomemcached.UPR_DELETION, gomemcached.UPR_EXPIRATION:
		// This should have been handled already in receiver goroutine.
		return 0, fmt.Errorf("unexpected data change, res: %#v", res)

	default:
		return 0, fmt.Errorf("unknown opcode, res: %#v", res)
	}

	return 1, nil
}

func (d *bucketDataSource) getVBucketMetaData(vbucketId uint16) (
	*VBucketMetaData, uint64, error) {
	atomic.AddUint64(&d.stats.TotGetVBucketMetaData, 1)

	buf, lastSeq, err := d.receiver.GetMetaData(vbucketId)
	if err != nil {
		atomic.AddUint64(&d.stats.TotGetVBucketMetaDataErr, 1)
		return nil, 0, err
	}

	vbucketMetaData := &VBucketMetaData{}
	if len(buf) > 0 {
		if err = json.Unmarshal(buf, vbucketMetaData); err != nil {
			atomic.AddUint64(&d.stats.TotGetVBucketMetaDataUnmarshalErr, 1)
			return nil, 0, err
		}
	}

	atomic.AddUint64(&d.stats.TotGetVBucketMetaDataOk, 1)
	return vbucketMetaData, lastSeq, nil
}

func (d *bucketDataSource) setVBucketMetaData(vbucketId uint16,
	v *VBucketMetaData) error {
	atomic.AddUint64(&d.stats.TotSetVBucketMetaData, 1)

	buf, err := json.Marshal(v)
	if err != nil {
		atomic.AddUint64(&d.stats.TotSetVBucketMetaDataMarshalErr, 1)
		return err
	}

	err = d.receiver.SetMetaData(vbucketId, buf)
	if err != nil {
		atomic.AddUint64(&d.stats.TotSetVBucketMetaDataErr, 1)
		return err
	}

	atomic.AddUint64(&d.stats.TotSetVBucketMetaDataOk, 1)
	return nil
}

func (d *bucketDataSource) sendStreamReq(sendCh chan *gomemcached.MCRequest,
	vbucketId uint16) error {
	vbucketMetaData, lastSeq, err := d.getVBucketMetaData(vbucketId)
	if err != nil {
		return fmt.Errorf("sendStreamReq, err: %v", err)
	}

	vbucketUUID := uint64(0)
	if len(vbucketMetaData.FailOverLog) >= 1 {
		vbucketUUID = vbucketMetaData.FailOverLog[len(vbucketMetaData.FailOverLog)-1][0]
	}

	seqStart := lastSeq
	seqEnd := uint64(0xffffffffffffffff)
	flags := uint32(0)

	// TODO: Parameterizing flags & seqEnd might allow for incremental backup?

	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.UPR_STREAMREQ,
		VBucket: vbucketId,
		Opaque:  uint32(vbucketId),
		Extras:  make([]byte, 48),
	}
	binary.BigEndian.PutUint32(req.Extras[:4], flags)
	binary.BigEndian.PutUint32(req.Extras[4:8], uint32(0)) // Reserved.
	binary.BigEndian.PutUint64(req.Extras[8:16], seqStart)
	binary.BigEndian.PutUint64(req.Extras[16:24], seqEnd)
	binary.BigEndian.PutUint64(req.Extras[24:32], vbucketUUID)
	binary.BigEndian.PutUint64(req.Extras[32:40], vbucketMetaData.SnapStart)
	binary.BigEndian.PutUint64(req.Extras[40:48], vbucketMetaData.SnapEnd)

	atomic.AddUint64(&d.stats.TotUPRStreamReq, 1)
	sendCh <- req

	return nil
}

func (d *bucketDataSource) Stats(dest *BucketDataSourceStats) error {
	d.stats.AtomicCopyTo(dest)
	return nil
}

func (d *bucketDataSource) Close() error {
	d.m.Lock()
	if d.life != "running" {
		d.m.Unlock()
		return fmt.Errorf("Close() called when not in running state: %s", d.life)
	}
	d.life = "closed"
	d.m.Unlock()

	// A close message to refreshClusterCh's goroutine will end
	// refreshWorkersCh's goroutine, which closes every workerCh and
	// then finally closes the closedCh.
	d.refreshClusterCh <- "CLOSE"

	<-d.closedCh

	// NOTE: By this point, worker goroutines might still be going,
	// but should end soon.
	return nil
}

func (d *bucketDataSource) Kick(reason string) error {
	go func() {
		if d.isRunning() {
			atomic.AddUint64(&d.stats.TotKick, 1)
			d.refreshClusterCh <- reason
			atomic.AddUint64(&d.stats.TotKickOk, 1)
		}
	}()

	return nil
}

// --------------------------------------------------------------

type bucketWrapper struct {
	b *couchbase.Bucket
}

func (bw *bucketWrapper) Close() {
	bw.b.Close()
}

func (bw *bucketWrapper) GetUUID() string {
	return bw.b.UUID
}

func (bw *bucketWrapper) VBServerMap() *couchbase.VBucketServerMap {
	return bw.b.VBServerMap()
}

// TODO: Use AUTH'ed approach.
func ConnectBucket(serverURL, poolName, bucketName, bucketUUID string,
	authFunc AuthFunc) (Bucket, error) {
	bucket, err := couchbase.GetBucket(serverURL, poolName, bucketName)
	if err != nil {
		return nil, err
	}
	if bucket == nil {
		return nil, fmt.Errorf("unknown bucket,"+
			" serverURL: %s, bucketName: %s, bucketUUID: %s, bucket.UUID: %s",
			serverURL, bucketName, bucketUUID, bucket.UUID)
	}
	if bucketUUID != "" && bucketUUID != bucket.UUID {
		bucket.Close()
		return nil, fmt.Errorf("mismatched bucket uuid,"+
			" serverURL: %s, bucketName: %s, bucketUUID: %s, bucket.UUID: %s",
			serverURL, bucketName, bucketUUID, bucket.UUID)
	}
	return &bucketWrapper{b: bucket}, nil
}

func UPROpen(mc *memcached.Client, name string, bufSize uint32) error {
	rq := &gomemcached.MCRequest{
		Opcode: gomemcached.UPR_OPEN,
		Key:    []byte(name),
		Opaque: 0xf00d1234,
		Extras: make([]byte, 8),
	}
	binary.BigEndian.PutUint32(rq.Extras[:4], 0) // First 4 bytes are reserved.
	flags := uint32(1)                           // NOTE: 1 for producer, 0 for consumer.
	binary.BigEndian.PutUint32(rq.Extras[4:], flags)

	if err := mc.Transmit(rq); err != nil {
		return fmt.Errorf("UPROpen transmit, err: %v", err)
	}
	res, err := mc.Receive()
	if err != nil {
		return fmt.Errorf("UPROpen receive, err: %v", err)
	}
	if res.Opcode != gomemcached.UPR_OPEN {
		return fmt.Errorf("UPROpen unexpected #opcode %v", res.Opcode)
	}
	if res.Opaque != rq.Opaque {
		return fmt.Errorf("UPROpen opaque mismatch, %v over %v", res.Opaque, res.Opaque)
	}
	if res.Status != gomemcached.SUCCESS {
		return fmt.Errorf("UPROpen failed, status: %v, %#v", res.Status, res)
	}
	if bufSize > 0 {
		rq := &gomemcached.MCRequest{
			Opcode: gomemcached.UPR_CONTROL,
			Key:    []byte("connection_buffer_size"),
			Body:   []byte(strconv.Itoa(int(bufSize))),
		}
		if err = mc.Transmit(rq); err != nil {
			return fmt.Errorf("UPROpen transmit UPR_CONTROL, err: %v", err)
		}
	}
	return nil
}

func ParseFailOverLog(body []byte) ([][]uint64, error) {
	if len(body)%16 != 0 {
		return nil, fmt.Errorf("invalid body length %v, in failover-log", len(body))
	}
	flog := make([][]uint64, len(body)/16)
	for i, j := 0, 0; i < len(body); i += 16 {
		uuid := binary.BigEndian.Uint64(body[i : i+8])
		seqn := binary.BigEndian.Uint64(body[i+8 : i+16])
		flog[j] = []uint64{uuid, seqn}
		j++
	}
	return flog, nil
}

// --------------------------------------------------------------

func (s *BucketDataSourceStats) AtomicCopyTo(r *BucketDataSourceStats) {
	// Using reflection rather than a whole slew of explicit
	// invocations of atomic.LoadUint64()/StoreUint64()'s.
	rve := reflect.ValueOf(r).Elem()
	sve := reflect.ValueOf(s).Elem()
	svet := sve.Type()
	for i := 0; i < svet.NumField(); i++ {
		rvef := rve.Field(i)
		svef := sve.Field(i)
		if rvef.CanAddr() && svef.CanAddr() {
			rvefp := rvef.Addr().Interface()
			svefp := svef.Addr().Interface()
			v := atomic.LoadUint64(svefp.(*uint64))
			atomic.StoreUint64(rvefp.(*uint64), v)
		}
	}
}

// --------------------------------------------------------------

// Calls f() in a loop, sleeping in an exponential backoff if needed.
// The provided f() function should return < 0 to stop the loop; >= 0
// to continue the loop, where > 0 means there was progress which
// allows an immediate retry of f() with no sleeping.  A return of < 0
// is useful when f() will never make any future progress.
func ExponentialBackoffLoop(name string,
	f func() int,
	startSleepMS int,
	backoffFactor float32,
	maxSleepMS int) {
	nextSleepMS := startSleepMS
	for {
		progress := f()
		if progress < 0 {
			return
		}
		if progress > 0 {
			// When there was some progress, we can reset nextSleepMS.
			nextSleepMS = startSleepMS
		} else {
			// If zero progress was made this cycle, then sleep.
			time.Sleep(time.Duration(nextSleepMS) * time.Millisecond)

			// Increase nextSleepMS in case next time also has 0 progress.
			nextSleepMS = int(float32(nextSleepMS) * backoffFactor)
			if nextSleepMS > maxSleepMS {
				nextSleepMS = maxSleepMS
			}
		}
	}
}
