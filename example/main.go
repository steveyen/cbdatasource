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

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/gomemcached"
	"github.com/steveyen/cbdatasource"
)

// Simple, memory-only sample program that uses the cbdatasource API's
// to get data from a couchbase cluster using DCP.

var serverURL = flag.String("serverURL", "http://localhost:8091",
	"couchbase server URL")
var poolName = flag.String("poolName", "default",
	"pool name")
var bucketName = flag.String("bucketName", "default",
	"bucket name")
var bucketUUID = flag.String("bucketUUID", "",
	"bucket UUID")
var vbucketIds = flag.String("vbucketIds", "",
	"comma separated vbucket id numbers; defaults to all vbucket id's")

var bds cbdatasource.BucketDataSource

func main() {
	flag.Parse()

	go dumpOnSignalForPlatform()

	serverURLs := []string{*serverURL}

	vbucketIdsArr := []uint16(nil) // A nil means get all the vbuckets.
	if *vbucketIds != "" {
		vbucketIdsArr = []uint16{}
		for _, vbucketIdStr := range strings.Split(*vbucketIds, ",") {
			if vbucketIdStr != "" {
				vbucketId, err := strconv.Atoi(vbucketIdStr)
				if err != nil {
					log.Fatalf("error: could not parse vbucketId: %s", vbucketIdStr)
				}
				vbucketIdsArr = append(vbucketIdsArr, uint16(vbucketId))
			}
		}
		if len(vbucketIdsArr) <= 0 {
			vbucketIdsArr = nil
		}
	}

	var options  *cbdatasource.BucketDataSourceOptions
	var authFunc cbdatasource.AuthFunc

	receiver := &ExampleReceiver{}

	b, err := cbdatasource.NewBucketDataSource(serverURLs,
		*poolName, *bucketName, *bucketUUID,
		vbucketIdsArr, authFunc, receiver, options)
	if err != nil {
		log.Fatalf(fmt.Sprintf("error: NewBucketDataSource, err: %v", err))
	}

	receiver.b = b
	bds = b

	if err = b.Start(); err != nil {
		log.Fatalf(fmt.Sprintf("error: Start, err: %v", err))
	}

	log.Printf("started bucket data source: %v", b)

	for {
		time.Sleep(1000 * time.Millisecond)
		reportStats(b, false)
	}
}

var mutexStats sync.Mutex
var lastStats = &cbdatasource.BucketDataSourceStats{}
var currStats = &cbdatasource.BucketDataSourceStats{}

func reportStats(b cbdatasource.BucketDataSource, force bool) {
	mutexStats.Lock()
	defer mutexStats.Unlock()

	b.Stats(currStats)
	if force || !reflect.DeepEqual(lastStats, currStats) {
		buf, err := json.Marshal(currStats)
		if err == nil {
			log.Printf("%s", string(buf))
		}
		lastStats, currStats = currStats, lastStats
	}
}

type ExampleReceiver struct {
	b cbdatasource.BucketDataSource

	m sync.Mutex

	seqs map[uint16]uint64 // To track max seq #'s we received per vbucketId.
	meta map[uint16][]byte // To track metadata blob's per vbucketId.
}

func (r *ExampleReceiver) OnError(err error) {
	log.Printf("error: %v", err)
	reportStats(r.b, true)
}

func (r *ExampleReceiver) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	res *gomemcached.MCResponse) error {
	log.Printf("data-update: vbucketId: %d, key: %s, seq: %x, res: %#v",
		vbucketId, key, seq, res)
	r.updateSeq(vbucketId, seq)
	return nil
}

func (r *ExampleReceiver) DataDelete(vbucketId uint16, key []byte, seq uint64,
	res *gomemcached.MCResponse) error {
	log.Printf("data-delete: vbucketId: %d, key: %s, seq: %x, res: %#v",
		vbucketId, key, seq, res)
	r.updateSeq(vbucketId, seq)
	return nil
}

func (r *ExampleReceiver) updateSeq(vbucketId uint16, seq uint64) {
	r.m.Lock()
	defer r.m.Unlock()

	if r.seqs == nil {
		r.seqs = make(map[uint16]uint64)
	}
	if r.seqs[vbucketId] < seq {
		r.seqs[vbucketId] = seq // Remember the max seq for GetMetaData().
	}
}

func (r *ExampleReceiver) SnapshotStart(vbucketId uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	log.Printf("snapshot-start: vbucketId: %d, snapStart: %x, snapEnd: %x, snapType: %x",
		vbucketId, snapStart, snapEnd, snapType)
	return nil
}

func (r *ExampleReceiver) SetMetaData(vbucketId uint16, value []byte) error {
	log.Printf("set-metadata: vbucketId: %d, value: %s", vbucketId, value)

	r.m.Lock()
	defer r.m.Unlock()

	if r.meta == nil {
		r.meta = make(map[uint16][]byte)
	}
	r.meta[vbucketId] = value

	return nil
}

func (r *ExampleReceiver) GetMetaData(vbucketId uint16) (
	value []byte, lastSeq uint64, err error) {
	log.Printf("get-metadata: vbucketId: %d", vbucketId)

	r.m.Lock()
	defer r.m.Unlock()

	value = []byte(nil)
	if r.meta != nil {
		value = r.meta[vbucketId]
	}

	if r.seqs != nil {
		lastSeq = r.seqs[vbucketId]
	}

	return value, lastSeq, nil
}

func (r *ExampleReceiver) Rollback(vbucketId uint16, rollbackSeq uint64) error {
	log.Printf("rollback: vbucketId: %d, rollbackSeq: %x", vbucketId, rollbackSeq)

	return fmt.Errorf("unimpl-rollback")
}

func dumpOnSignal(signals ...os.Signal) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, signals...)
	for _ = range c {
		reportStats(bds, true)

		log.Printf("dump: goroutine...")
		pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
		log.Printf("dump: heap...")
		pprof.Lookup("heap").WriteTo(os.Stderr, 1)
	}
}
