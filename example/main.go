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
	"flag"
	"fmt"
	"log"
	"sync"

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

func main() {
	serverURLs := []string{*serverURL}
	vbucketIds := []uint16(nil) // A nil means get all the vbuckets.

	var authFunc cbdatasource.AuthFunc
	var options *cbdatasource.BucketDataSourceOptions

	receiver := &ExampleReceiver{}

	b, err := cbdatasource.NewBucketDataSource(serverURLs,
		*poolName, *bucketName, *bucketUUID,
		vbucketIds, authFunc, receiver, options)
	if err != nil {
		log.Fatalf(fmt.Sprintf("error: NewBucketDataSource, err: %v", err))
	}

	if err = b.Start(); err != nil {
		log.Fatalf(fmt.Sprintf("error: Start, err: %v", err))
	}

	log.Printf("started bucket data source: %v", b)

	select {}
}

type ExampleReceiver struct {
	m sync.Mutex

	seqs map[uint16]uint64
	meta map[uint16][]byte
}

func (r *ExampleReceiver) OnError(err error) {
	log.Printf("error: %v", err)
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
