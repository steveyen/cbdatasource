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
	"sync"
	"time"
)

type Receiver interface {
	OnError(error)
	GetMetaData() ([]byte, error)
	SetMetaData([]byte) error
	OnDocUpdate()
	OnDocDelete()
	GetSeqNum()
	Snapshot()
	Rollback()
}

type BucketDataSource interface {
	Start() error
	Stats() BucketDataSourceStats
	Close() error
}

type BucketDataSourceOptions struct {
	ClusterManagerBackoffFactor float32
	ClusterManagerSleepInitMS int
	ClusterManagerSleepMaxMS int

	DataManagerBackoffFactor float32
	DataManagerSleepInitMS int
	DataManagerSleepMaxMS int
}

var DefaultBucketDataSourceOptions =  &BucketDataSourceOptions{
	ClusterManagerBackoffFactor: 1.5,
	ClusterManagerSleepInitMS: 100,
	ClusterManagerSleepMaxMS: 1000,

	DataManagerBackoffFactor: 1.5,
	DataManagerSleepInitMS: 100,
	DataManagerSleepMaxMS: 1000,
}

type BucketDataSourceStats struct {
}

type bucketDataSource struct {
	serverURLs []string
	bucketName string
	bucketUUID string
	vbucketIds []uint16
	authFunc   AuthFunc
	receiver   Receiver
	options    *BucketDataSourceOptions

	m         sync.Mutex
	isRunning bool
}

type AuthFunc func(kind string, challenge []byte) (response []byte, err error)

func NewBucketDataSource(serverURLs []string,
	bucketName, bucketUUID string,
	vbucketIds []uint16, authFunc AuthFunc,
	receiver Receiver, options *BucketDataSourceOptions) (BucketDataSource, error) {
	if len(serverURLs) < 1 {
		return nil, fmt.Errorf("missing at least 1 serverURL")
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
		bucketName: bucketName,
		bucketUUID: bucketUUID,
		vbucketIds: vbucketIds,
		authFunc:   authFunc,
		receiver:   receiver,
		options:    options,
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

	go ExponentialBackoffLoop("bucketDataSource.run",
		func() int { return d.run() },
		int(sleepInitMS), backoffFactor, int(sleepMaxMS))

	return nil
}

func (d *bucketDataSource) run() int {
	return -1
}

func (d *bucketDataSource) Stats() BucketDataSourceStats {
	return BucketDataSourceStats{}
}

func (d *bucketDataSource) Close() error {
	return nil
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
