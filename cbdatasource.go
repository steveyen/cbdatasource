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
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/couchbase/gomemcached/client"
	"github.com/couchbaselabs/go-couchbase"
)

type Receiver interface {
	OnError(error) error
	GetMetaData() ([]byte, error)
	SetMetaData([]byte) error
	OnDocUpdate() error
	OnDocDelete() error
	Snapshot() error
	Rollback() error
}

type BucketDataSource interface {
	Start() error
	Stats() BucketDataSourceStats
	Close() error
}

type BucketDataSourceOptions struct {
	// Used during DCP stream setup
	Name string

	// Factor (like 1.5) to increase sleep time between retries
	// in connecting to get a cluster map from the cluster manager.
	ClusterManagerBackoffFactor float32

	// Initial sleep time before the first retry to the cluster manager.
	ClusterManagerSleepInitMS int

	// Maximum sleep time between retries to the cluster manager.
	ClusterManagerSleepMaxMS int

	// Factor (like 1.5) to increase sleep time between retries
	// in connecting to a data manager node.
	DataManagerBackoffFactor float32

	// Initial sleep time before the first retry to a data manager.
	DataManagerSleepInitMS int

	// Maximum sleep time between retries to the data manager.
	DataManagerSleepMaxMS int
}

var DefaultBucketDataSourceOptions = &BucketDataSourceOptions{
	ClusterManagerBackoffFactor: 1.5,
	ClusterManagerSleepInitMS:   100,
	ClusterManagerSleepMaxMS:    1000,

	DataManagerBackoffFactor: 1.5,
	DataManagerSleepInitMS:   100,
	DataManagerSleepMaxMS:    1000,
}

type BucketDataSourceStats struct {
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
	refreshStreamsCh chan string

	m         sync.Mutex
	isRunning bool
	vbm       *couchbase.VBucketServerMap
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
		refreshStreamsCh: make(chan string, 1),

		isRunning: false,
	}, nil
}

func (d *bucketDataSource) Start() error {
	d.m.Lock()
	defer d.m.Unlock()

	if d.isRunning {
		return fmt.Errorf("already running")
	}
	d.isRunning = true

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

	go ExponentialBackoffLoop("bucketDataSource.clusterManagerOnce",
		func() int { return d.refreshCluster() },
		sleepInitMS, backoffFactor, sleepMaxMS)

	go d.refreshStreams()

	return nil
}

func (d *bucketDataSource) refreshCluster() int {
serverURLs:
	for _, serverURL := range d.serverURLs {
		bucket, err := getBucket(serverURL,
			d.poolName, d.bucketName, d.bucketUUID, d.authFunc)
		if err != nil {
			err = d.receiver.OnError(err)
			if err != nil {
				return -1
			}
			continue serverURLs // Try another serverURL.
		}
		vbm := bucket.VBServerMap()
		if vbm == nil {
			bucket.Close()
			err := d.receiver.OnError(fmt.Errorf("no vbm,"+
				" serverURL: %s, bucketName: %s, bucketUUID: %s, bucket.UUID: %s",
				serverURL, d.bucketName, d.bucketUUID, bucket.UUID))
			if err != nil {
				return -1
			}
			continue serverURLs
		}
		bucket.Close()

		d.m.Lock()
		vbmSame := reflect.DeepEqual(vbm, d.vbm)
		d.vbm = vbm
		d.m.Unlock()

		if !vbmSame {
			d.refreshStreamsCh <- "new-vbm"
		}

		_, alive := <-d.refreshClusterCh
		if !alive {
			return -1
		}

		return 1
	}

	return 0 // Ran through all the servers, so no progress.
}

func (d *bucketDataSource) refreshStreams() {
	workers := make(map[string]chan []uint16)

	for _ = range d.refreshStreamsCh {
		d.m.Lock()
		vbm := d.vbm
		d.m.Unlock()

		// Group the vbucketIds by server.
		vbucketIdsByServer := make(map[string][]uint16)

		for _, vbucketId := range d.vbucketIds {
			if int(vbucketId) >= len(vbm.VBucketMap) {
				d.receiver.OnError(fmt.Errorf("refreshStreams"+
					" saw bad vbucketId: %d", vbucketId))
				continue
			}
			serverIdxs := vbm.VBucketMap[vbucketId]
			if serverIdxs == nil || len(serverIdxs) < 1 {
				d.receiver.OnError(fmt.Errorf("refreshStreams"+
					" no serverIdxs for vbucketId: %d", vbucketId))
				continue
			}
			masterIdx := serverIdxs[0]
			if int(masterIdx) >= len(vbm.ServerList) {
				d.receiver.OnError(fmt.Errorf("refreshStreams"+
					" no masterIdx for vbucketId: %d", vbucketId))
				continue
			}
			masterServer := vbm.ServerList[masterIdx]
			if masterServer == "" {
				d.receiver.OnError(fmt.Errorf("refreshStreams"+
					" no masterServer for vbucketId: %d", vbucketId))
				continue
			}
			v, exists := vbucketIdsByServer[masterServer]
			if !exists || v == nil {
				v = []uint16{}
			}
			v = append(v, vbucketId)
			vbucketIdsByServer[masterServer] = v
		}

		// Start any missing workers and update workers with their
		// latest vbucketIds.
		for server, serverVBucketIds := range vbucketIdsByServer {
			workerCh, exists := workers[server]
			if !exists || workerCh == nil {
				workerCh = make(chan []uint16)
				workers[server] = workerCh
				go d.workerStart(server, workerCh)
			}

			workerCh <- serverVBucketIds
		}

		// Close any extraneous workers.
		for server, workerCh := range workers {
			if _, exists := vbucketIdsByServer[server]; !exists {
				delete(workers, server)
				close(workerCh)
			}
		}
	}

	for _, workerCh := range workers {
		close(workerCh)
	}
}

func (d *bucketDataSource) workerStart(server string, newVBucketIdsCh chan []uint16) {
	curVBucketIds := []uint16{}

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

	name := "cbdatasource"

	go ExponentialBackoffLoop(name,
		func() int {
			return -1
		},
		sleepInitMS, backoffFactor, sleepMaxMS)

	for newVBucketIds := range newVBucketIdsCh {
		if reflect.DeepEqual(curVBucketIds, newVBucketIds) {
			continue // The vbucketIds list hasn't changed, so no-op.
		}

		// TODO: start stream and manage it.
	}
}

func (d *bucketDataSource) Stats() BucketDataSourceStats {
	return BucketDataSourceStats{}
}

func (d *bucketDataSource) Close() error {
	return nil
}

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

func connectDataNode(hostPort string, authFunc AuthFunc) (*memcached.Client, error) {
	// TODO: Use authFunc.
	return memcached.Connect("tcp", hostPort)
}

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
