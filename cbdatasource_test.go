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
	"testing"

	"github.com/couchbase/gomemcached"
)

type TestReceiver struct {
}

func (r *TestReceiver) OnError(error) {
}

	// Invoked by the BucketDataSource when it has received a mutation
	// from the data source.
func (r *TestReceiver) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	res *gomemcached.MCResponse) error {
	return nil
}

	// Invoked by the BucketDataSource when it has received a deletion
	// or expiration from the data source.
func (r *TestReceiver) DataDelete(vbucketId uint16, key []byte, seq uint64,
	res *gomemcached.MCResponse) error {
	return nil
}

func (r *TestReceiver) SnapshotStart(vbucketId uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	return nil
}

func (r *TestReceiver) SetMetaData(vbucketId uint16, value []byte) error {
	return nil
}

func (r *TestReceiver) GetMetaData(vbucketId uint16) (value []byte, lastSeq uint64, err error) {
	return nil, 0, nil
}

func (r *TestReceiver) Rollback(vbucketId uint16, rollbackSeq uint64) error {
	return nil
}

func TestNilParams(t *testing.T) {
	serverURLs := []string(nil)
	bucketUUID := ""
	vbucketIds := []uint16(nil)
	var authFunc AuthFunc
	var receiver Receiver
	var options *BucketDataSourceOptions

	bds, err := NewBucketDataSource(serverURLs, "poolName", "bucketName", bucketUUID,
		vbucketIds, authFunc, receiver, options)
	if err == nil || bds != nil {
		t.Errorf("expected err")
	}

	serverURLs = []string{"foo"}
	bucketUUID = ""
	vbucketIds = []uint16(nil)

	bds, err = NewBucketDataSource(serverURLs, "poolName", "bucketName", bucketUUID,
		vbucketIds, authFunc, receiver, options)
	if err == nil || bds != nil {
		t.Errorf("expected err")
	}

	receiver = &TestReceiver{}
	bds, err = NewBucketDataSource(serverURLs, "poolName", "bucketName", bucketUUID,
		vbucketIds, authFunc, receiver, options)
	if err != nil || bds == nil {
		t.Errorf("expected no err")
	}
}
