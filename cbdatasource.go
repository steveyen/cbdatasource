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
	"time"

	"github.com/couchbase/gomemcached"
	"github.com/couchbase/gomemcached/client"
	"github.com/couchbaselabs/go-couchbase"
)

type Receiver interface {
	// Invoked in advisory fashion by the BucketDataSource when it
	// encounters an error.  The BucketDataSource will continue to try
	// to "heal" and restart connections, etc, as necessary.  The
	// Receiver has a recourse during these error notifications of
	// simply Close()'ing the BucketDataSource.
	OnError(error)

	// Invoked by the BucketDataSource when it has received a mutation
	// from the data source.
	DataUpdate(vbucketId uint16, key []byte, seq uint64,
		r *gomemcached.MCResponse) error

	// Invoked by the BucketDataSource when it has received a deletion
	// or expiration from the data source.
	DataDelete(vbucketId uint16, key []byte, seq uint64,
		r *gomemcached.MCResponse) error

	// An advisory callback invoked by the BucketDataSource when it
	// has received a start snapshot message from the data source.
	// The Receiver implementation may choose to optimize persistence
	// commits for previous snapshots as part of this callback (e.g.,
	// commit any batch write for the previous snapshot).  Or the
	// Receiver implementation may choose to do nothing.
	Snapshot(vbucketId uint16, snapStart, snapEnd uint64, snapType uint32) error

	// The Receiver should persist the value parameter of
	// SetMetaData() for retrieval during some future call to
	// GetMetaData(), to help with restarts of DCP streams.
	SetMetaData(vbucketId uint16, value []byte) error

	// GetMetaData() should return the opaque value previously
	// provided by an earlier call to SetMetaData().  If there was no
	// previous call to SetMetaData(), such as in the case of a brand
	// new instance of a Receiver (as opposed to a restarted or
	// reloaded Receiver), the Receiver should return (nil, 0, nil)
	// for (value, lastSeq, err), respectively.  The lastSeq should be
	// the last sequence number persisted during calls to DataUpdate()
	// / DataDelete().
	GetMetaData(vbucketId uint16) (value []byte, lastSeq uint64, err error)

	// Invoked by the BucketDataSource when the datasource signals a
	// rollback during stream initialization.
	Rollback(vbucketId uint16, rollbackSeq uint64) error
}

type VBucketMetaData struct {
	VBucketId   uint16 `json:"vbucketId"`
	SeqStart    uint64 `json:"seqStart"`
	SeqEnd      uint64 `json:"seqEnd"`
	SnapStart   uint64 `json:"snapStart"`
	SnapEnd     uint64 `json:"snapEnd"`
	FailOverLog [][]uint64
}

type BucketDataSource interface {
	Start() error
	Stats() BucketDataSourceStats
	Close() error
}

type BucketDataSourceOptions struct {
	// Used during UPR_OPEN stream setup and other debugging messsages.
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

	// Provided to UPR flow control / buffer size.
	FeedBufferSizeBytes uint32

	// Used for UPR flow control and buffer-ack when this percentage
	// of FeedBufferSizeBytes is reached.
	FeedBufferAckThreshold float32
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

type BucketDataSourceStats struct {
	// TODO.
}

// --------------------------------------------------------

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

	m    sync.Mutex // Protects all the below fields.
	life string     // Valid life states: "" (unstarted); "running"; "closed".
	vbm  *couchbase.VBucketServerMap
}

type AuthFunc func(kind string, challenge []byte) (response []byte, err error)

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
	}, nil
}

func (d *bucketDataSource) Start() error {
	d.m.Lock()
	defer d.m.Unlock()

	if d.life != "" {
		return fmt.Errorf("Start() called in wrong state: %s", d.life)
	}
	d.life = "running"

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
	}()

	go d.refreshWorkers()

	return nil
}

func (d *bucketDataSource) refreshCluster() int {
	for _, serverURL := range d.serverURLs {
		bucket, err := getBucket(serverURL,
			d.poolName, d.bucketName, d.bucketUUID, d.authFunc)
		if err != nil {
			d.receiver.OnError(err)
			continue // Try another serverURL.
		}
		vbm := bucket.VBServerMap()
		if vbm == nil {
			bucket.Close()
			d.receiver.OnError(fmt.Errorf("no vbm,"+
				" serverURL: %s, bucketName: %s, bucketUUID: %s, bucket.UUID: %s",
				serverURL, d.bucketName, d.bucketUUID, bucket.UUID))
			continue // Try another serverURL.
		}
		bucket.Close()

		d.m.Lock()
		vbmSame := reflect.DeepEqual(vbm, d.vbm)
		d.vbm = vbm
		d.m.Unlock()

		if !vbmSame {
			d.refreshWorkersCh <- "new-vbm" // Kick the workers to refresh.
		}

		_, alive := <-d.refreshClusterCh // Wait for a refresh kick.
		if !alive {                      // Or, if we're closed then shutdown.
			return -1
		}

		return 1 // We had progress, so restart at the first serverURL.
	}

	return 0 // Ran through all the serverURLs, so no progress.
}

func (d *bucketDataSource) refreshWorkers() {
	workers := make(map[string]chan []uint16) // Keyed by server.

	for _ = range d.refreshWorkersCh { // Wait for a refresh kick.
		d.m.Lock()
		vbm := d.vbm
		d.m.Unlock()

		// Group the vbm's vbucketIds by server.
		vbucketIdsByServer := make(map[string][]uint16)

		for _, vbucketId := range d.vbucketIds {
			if int(vbucketId) >= len(vbm.VBucketMap) {
				d.receiver.OnError(fmt.Errorf("refreshWorkers"+
					" saw bad vbucketId: %d", vbucketId))
				continue
			}
			serverIdxs := vbm.VBucketMap[vbucketId]
			if serverIdxs == nil || len(serverIdxs) < 1 {
				d.receiver.OnError(fmt.Errorf("refreshWorkers"+
					" no serverIdxs for vbucketId: %d", vbucketId))
				continue
			}
			masterIdx := serverIdxs[0]
			if int(masterIdx) >= len(vbm.ServerList) {
				d.receiver.OnError(fmt.Errorf("refreshWorkers"+
					" no masterIdx for vbucketId: %d", vbucketId))
				continue
			}
			masterServer := vbm.ServerList[masterIdx]
			if masterServer == "" {
				d.receiver.OnError(fmt.Errorf("refreshWorkers"+
					" no masterServer for vbucketId: %d", vbucketId))
				continue
			}
			v, exists := vbucketIdsByServer[masterServer]
			if !exists || v == nil {
				v = []uint16{}
			}
			vbucketIdsByServer[masterServer] = append(v, vbucketId)
		}

		// Close any extraneous workers.
		for server, workerCh := range workers {
			if _, exists := vbucketIdsByServer[server]; !exists {
				delete(workers, server)
				close(workerCh)
			}
		}

		// Start any missing workers and update workers with their
		// latest vbucketIds.
		for server, serverVBucketIds := range vbucketIdsByServer {
			workerCh, exists := workers[server]
			if !exists || workerCh == nil {
				workerCh = make(chan []uint16)
				workers[server] = workerCh
				d.workerStart(server, workerCh)
			}

			workerCh <- serverVBucketIds
		}
	}

	// We reach here when we need to shutdown.
	for _, workerCh := range workers {
		close(workerCh)
	}
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

	go ExponentialBackoffLoop("cbdatasource.worker",
		func() int { return d.worker(server, workerCh) },
		sleepInitMS, backoffFactor, sleepMaxMS)
}

func (d *bucketDataSource) worker(server string, workerCh chan []uint16) int {
	client, err := memcached.Connect("tcp", server)
	if err != nil {
		d.receiver.OnError(err)
		return 0
	}
	defer client.Close()

	// TODO: Call client.Auth(user, pswd).

	uprOpenName := d.options.Name
	if uprOpenName == "" {
		uprOpenName = fmt.Sprintf("cbdatasource-%x", rand.Int63())
	}

	err = UPROpen(client, uprOpenName, d.options.FeedBufferSizeBytes)
	if err != nil {
		d.receiver.OnError(err)
		return 0
	}

	sendErrCh := make(chan bool)
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
		for msg := range sendCh {
			err := client.Transmit(msg)
			if err != nil {
				close(sendErrCh)
				d.receiver.OnError(err)
				return
			}
		}
	}()

	go func() { // Receiver goroutine.
		var hdr [gomemcached.HDR_LEN]byte
		var pkt gomemcached.MCRequest

		conn := client.Hijack()

		for {
			// TODO: memory allocation here.
			_, err := pkt.Receive(conn, hdr[:])
			if err != nil {
				close(recvCh)
				d.receiver.OnError(err)
				return
			}
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
	}()

	// Values for vbucketId state in currVBucketIds:
	// "" (dead/closed/unknown), "requested", "running", "closing".
	currVBucketIds := map[uint16]string{}
	recvBytesTotal := uint32(0)
	ackBytes :=
		uint32(d.options.FeedBufferAckThreshold * float32(d.options.FeedBufferSizeBytes))

	for {
		select {
		case <-sendErrCh:
			return cleanup(1, nil) // We saw disconnect; assume we made progress.

		case res, alive := <-recvCh:
			if !alive {
				return cleanup(1, nil) // We saw disconnect; assume we made progress.
			}

			vbucketId := uint16(res.Opaque)
			vbucketIdState := currVBucketIds[vbucketId]

			fmt.Printf("worker: vbucketId: %d, vbucketIdState: %s, res: %#v",
				vbucketId, vbucketIdState, res)

			if vbucketIdState == "" {
				return cleanup(1, fmt.Errorf("unknown state,"+
					" vbucketId: %d, res: %#v", vbucketId, res))
			}

			switch res.Opcode {
			case gomemcached.UPR_MUTATION,
				gomemcached.UPR_DELETION,
				gomemcached.UPR_EXPIRATION:
				if vbucketIdState != "running" {
					return cleanup(1, fmt.Errorf("state not running,"+
						" vbucketId: %d, res: %#v", vbucketId, res))
				}

				seq := binary.BigEndian.Uint64(res.Extras[:8])
				if res.Opcode == gomemcached.UPR_MUTATION {
					err = d.receiver.DataUpdate(vbucketId, res.Key, seq, res)
				} else {
					err = d.receiver.DataDelete(vbucketId, res.Key, seq, res)
				}
				if err != nil {
					return cleanup(1, err)
				}

			case gomemcached.UPR_NOOP:
				sendCh <- &gomemcached.MCRequest{Opcode: gomemcached.UPR_NOOP}

			case gomemcached.UPR_STREAMREQ:
				delete(currVBucketIds, vbucketId)

				if vbucketIdState != "requested" {
					return cleanup(1, fmt.Errorf("bad streamreq state,"+
						" vbucketId: %d, res: %#v", vbucketId, res))
				}

				if res.Status != gomemcached.SUCCESS {
					if res.Status == gomemcached.ROLLBACK {
						if len(res.Extras) != 8 {
							return cleanup(1, fmt.Errorf("invalid rollback extras: %v\n",
								res.Extras))
						}

						rollbackSeq := binary.BigEndian.Uint64(res.Extras)
						err := d.receiver.Rollback(vbucketId, rollbackSeq)
						if err != nil {
							return cleanup(0, err)
						}

						currVBucketIds[vbucketId] = "requested"
						err = d.sendStreamReq(sendCh, vbucketId)
						if err != nil {
							return cleanup(0, err)
						}
					} else {
						// Maybe the vbucket moved, so kick off a cluster refresh.
						go func() { d.refreshClusterCh <- "stream-req-error" }()
					}
				} else { // SUCCESS case.
					flog, err := ParseFailOverLog(res.Body[:])
					if err != nil {
						return cleanup(1, err)
					}
					v, _, err := d.getVBucketMetaData(vbucketId)
					if err != nil {
						return cleanup(1, err)
					}

					v.FailOverLog = flog

					err = d.setVBucketMetaData(vbucketId, v)
					if err != nil {
						return cleanup(1, err)
					}

					currVBucketIds[vbucketId] = "running"
				}

			case gomemcached.UPR_STREAMEND:
				delete(currVBucketIds, vbucketId)

				// We should not normally see a stream-end, unless we
				// were trying to close.  Maybe the vbucket moved, so
				// kick off a cluster refresh.
				if vbucketIdState != "closing" {
					go func() { d.refreshClusterCh <- "stream-end" }()
				}

			case gomemcached.UPR_SNAPSHOT:
				if len(res.Extras) < 20 {
					return cleanup(1, fmt.Errorf("wrong snapshot extras, res: %#v", res))
				}
				snapStart := binary.BigEndian.Uint64(res.Extras[0:8])
				snapEnd := binary.BigEndian.Uint64(res.Extras[8:16])
				snapType := binary.BigEndian.Uint32(res.Extras[16:20])

				err = d.receiver.Snapshot(vbucketId, snapStart, snapEnd, snapType)
				if err != nil {
					return cleanup(1, err)
				}

				// TODO: Do we need to handle snapAck flag in snapType?

			case gomemcached.UPR_CONTROL:
				if res.Status != gomemcached.SUCCESS {
					return cleanup(1, fmt.Errorf("not success control: %#v", res))
				}

			case gomemcached.UPR_BUFFERACK:
				if res.Status != gomemcached.SUCCESS {
					return cleanup(1, fmt.Errorf("not success bufferack: %#v", res))
				}

			case gomemcached.UPR_FLUSH:
				panic(fmt.Sprintf("unimplemented flush, res: %#v", res))

			case gomemcached.UPR_OPEN:
				// Opening was long ago, so we should not see UPR_OPEN responses.
				panic(fmt.Sprintf("unexpected upr_open, res: %#v", res))

			case gomemcached.UPR_ADDSTREAM:
				// This normally comes from ns-server / dcp-migrator.
				panic(fmt.Sprintf("unexpected upr_addstream, res: %#v", res))

			case gomemcached.UPR_CLOSESTREAM:
				// Shouldn't see this, as producers (oddly!) respond
				// with a STREAM_END to our CLOSE_STREAM request.
				panic(fmt.Sprintf("unexpected upr_closestream, res: %#v", res))

			default:
				panic(fmt.Sprintf("unknown opcode, res: %#v", res))
			}

			recvBytesTotal +=
				uint32(gomemcached.HDR_LEN) +
					uint32(len(res.Key)+len(res.Extras)+len(res.Body))
			if ackBytes > 0 && recvBytesTotal > ackBytes {
				ack := &gomemcached.MCRequest{Opcode: gomemcached.UPR_BUFFERACK}
				ack.Extras = make([]byte, 4) // TODO: Memory mgmt.
				binary.BigEndian.PutUint32(ack.Extras, uint32(recvBytesTotal))
				sendCh <- ack
				recvBytesTotal = 0
			}

		case wantVBucketIdsArr, alive := <-workerCh:
			if !alive {
				return cleanup(-1, nil) // We've been asked to shutdown.
			}

			wantVBucketIds := map[uint16]bool{}
			for _, wantVBucketId := range wantVBucketIdsArr {
				wantVBucketIds[wantVBucketId] = true
			}

			for currVBucketId, state := range currVBucketIds {
				if !wantVBucketIds[currVBucketId] {
					if state != "" && state != "closing" {
						currVBucketIds[currVBucketId] = "closing"
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
					return cleanup(1, fmt.Errorf("wanting a closing vbucket: %d",
						wantVBucketId))
				}
				if state == "" {
					currVBucketIds[wantVBucketId] = "requested"
					err := d.sendStreamReq(sendCh, wantVBucketId)
					if err != nil {
						return cleanup(1, err)
					}
				} // Else, state of "requested" or "running", so no-op.
			}
		}
	}

	return -1 // Unreached.
}

func (d *bucketDataSource) getVBucketMetaData(vbucketId uint16) (
	*VBucketMetaData, uint64, error) {
	buf, lastSeq, err := d.receiver.GetMetaData(vbucketId)
	if err != nil {
		return nil, 0, err
	}

	vbucketMetaData := &VBucketMetaData{}
	if len(buf) > 0 {
		if err = json.Unmarshal(buf, vbucketMetaData); err != nil {
			return nil, 0, err
		}
	}

	return vbucketMetaData, lastSeq, nil
}

func (d *bucketDataSource) setVBucketMetaData(vbucketId uint16,
	v *VBucketMetaData) error {
	buf, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return d.receiver.SetMetaData(vbucketId, buf)
}

func (d *bucketDataSource) sendStreamReq(sendCh chan *gomemcached.MCRequest,
	vbucketId uint16) error {
	vbucketMetaData, lastSeq, err := d.getVBucketMetaData(vbucketId)
	if err != nil {
		return err
	}

	flags := uint32(0)

	uuid := uint64(0)
	if len(vbucketMetaData.FailOverLog) >= 1 {
		uuid = vbucketMetaData.FailOverLog[len(vbucketMetaData.FailOverLog)-1][0]
	}

	sendCh <- UprStreamReq(vbucketId, flags, uuid, lastSeq, 0xffffffffffffffff,
		vbucketMetaData.SnapStart, vbucketMetaData.SnapEnd)

	return nil
}

func (d *bucketDataSource) Stats() BucketDataSourceStats {
	return BucketDataSourceStats{}
}

func (d *bucketDataSource) Close() error {
	d.m.Lock()
	defer d.m.Unlock()
	if d.life != "running" {
		return fmt.Errorf("Close() called when not in running state: %s", d.life)
	}
	d.life = "closed"
	close(d.refreshClusterCh)
	return nil
}

// --------------------------------------------------------------

// TODO: Use AUTH'ed approach.
func getBucket(serverURL, poolName, bucketName, bucketUUID string,
	authFunc AuthFunc) (
	*couchbase.Bucket, error) {
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
	return bucket, nil
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
		return err
	}
	res, err := mc.Receive()
	if err != nil {
		return err
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
			return err
		}
	}
	return nil
}

func UprStreamReq(vbucketId uint16, flags uint32, vbucketUUID,
	seqStart, seqEnd, snapStart, snapEnd uint64) *gomemcached.MCRequest {
	rq := &gomemcached.MCRequest{
		Opcode:  gomemcached.UPR_STREAMREQ,
		VBucket: vbucketId,
		Opaque:  uint32(vbucketId),
	}
	rq.Extras = make([]byte, 48)
	binary.BigEndian.PutUint32(rq.Extras[:4], flags)
	binary.BigEndian.PutUint32(rq.Extras[4:8], uint32(0)) // Reserved.
	binary.BigEndian.PutUint64(rq.Extras[8:16], seqStart)
	binary.BigEndian.PutUint64(rq.Extras[16:24], seqEnd)
	binary.BigEndian.PutUint64(rq.Extras[24:32], vbucketUUID)
	binary.BigEndian.PutUint64(rq.Extras[32:40], snapStart)
	binary.BigEndian.PutUint64(rq.Extras[40:48], snapEnd)
	return rq
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
			// log.Printf("backoff: %s, progress: %d", name, progress)
			nextSleepMS = startSleepMS
		} else {
			// If zero progress was made this cycle, then sleep.
			// log.Printf("backoff: %s, sleep: %d (ms)", name, nextSleepMS)
			time.Sleep(time.Duration(nextSleepMS) * time.Millisecond)

			// Increase nextSleepMS in case next time also has 0 progress.
			nextSleepMS = int(float32(nextSleepMS) * backoffFactor)
			if nextSleepMS > maxSleepMS {
				nextSleepMS = maxSleepMS
			}
		}
	}
}
