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
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/couchbase/gomemcached"
	"github.com/couchbase/gomemcached/client"
	"github.com/couchbaselabs/go-couchbase"
)

type TestBucket struct {
	uuid     string
	vbsm     *couchbase.VBucketServerMap
	numClose int
}

func (bw *TestBucket) Close() {
	bw.numClose += 1
}

func (bw *TestBucket) GetUUID() string {
	return bw.uuid
}

func (bw *TestBucket) VBServerMap() *couchbase.VBucketServerMap {
	return bw.vbsm
}

type TestMutation struct {
	delete    bool
	vbucketId uint16
	key       []byte
	seq       uint64
	res       *gomemcached.MCResponse
}

type TestReceiver struct {
	errs []error
	muts []*TestMutation
	meta map[uint16][]byte

	numSnapshotStarts int
	numSetMetaDatas   int
	numGetMetaDatas   int
	numRollbacks      int

	testName string
}

func (r *TestReceiver) OnError(err error) {
	// fmt.Printf("  testName: %s: %v\n", r.testName, err)
	r.errs = append(r.errs, err)
}

func (r *TestReceiver) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	res *gomemcached.MCResponse) error {
	r.muts = append(r.muts, &TestMutation{
		delete:    false,
		vbucketId: vbucketId,
		key:       key,
		seq:       seq,
		res:       res,
	})
	return nil
}

func (r *TestReceiver) DataDelete(vbucketId uint16, key []byte, seq uint64,
	res *gomemcached.MCResponse) error {
	r.muts = append(r.muts, &TestMutation{
		delete:    true,
		vbucketId: vbucketId,
		key:       key,
		seq:       seq,
		res:       res,
	})
	return nil
}

func (r *TestReceiver) SnapshotStart(vbucketId uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	r.numSnapshotStarts += 1
	return nil
}

func (r *TestReceiver) SetMetaData(vbucketId uint16, value []byte) error {
	r.numSetMetaDatas += 1
	if r.meta == nil {
		r.meta = make(map[uint16][]byte)
	}
	r.meta[vbucketId] = value
	return nil
}

func (r *TestReceiver) GetMetaData(vbucketId uint16) (value []byte, lastSeq uint64, err error) {
	r.numGetMetaDatas += 1
	rv := []byte(nil)
	if r.meta != nil {
		rv = r.meta[vbucketId]
	}
	for i := len(r.muts) - 1; i >= 0; i = i - 1 {
		if r.muts[i].vbucketId == vbucketId {
			return rv, r.muts[i].seq, nil
		}
	}
	return rv, 0, nil
}

func (r *TestReceiver) Rollback(vbucketId uint16, rollbackSeq uint64) error {
	r.numRollbacks += 1
	return fmt.Errorf("bad-rollback")
}

// Implements ReadWriteCloser interface for fake networking.
type TestRWC struct {
	name      string
	numReads  int
	numWrites int
	readCh    chan RWReq
	writeCh   chan RWReq
}

func (c *TestRWC) Read(p []byte) (n int, err error) {
	c.numReads += 1
	if c.readCh != nil {
		resCh := make(chan RWRes)
		c.readCh <- RWReq{op: "read", buf: p, resCh: resCh}
		res := <- resCh
		return res.n, res.err
	}
	return 0, fmt.Errorf("fake-read-err")
}

func (c *TestRWC) Write(p []byte) (n int, err error) {
	c.numWrites += 1
	if c.writeCh != nil {
		resCh := make(chan RWRes)
		c.writeCh <- RWReq{op: "write", buf: p, resCh: resCh}
		res := <- resCh
		return res.n, res.err
	}
	return 0, fmt.Errorf("fake-write-err")
}

func (c *TestRWC) Close() error {
	if c.readCh != nil {
		close(c.readCh)
		c.readCh = nil
	}
	if c.writeCh != nil {
		close(c.writeCh)
		c.writeCh = nil
	}
	return nil
}

type RWReq struct {
	op  string
	buf []byte
	resCh chan RWRes
}

type RWRes struct {
	n   int
	err error
}

// ------------------------------------------------------

func TestNewBucketDataSource(t *testing.T) {
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

	poolName := ""
	bds, err = NewBucketDataSource(serverURLs, poolName, "bucketName", bucketUUID,
		vbucketIds, authFunc, receiver, options)
	if err == nil || bds != nil {
		t.Errorf("expected err")
	}

	bucketName := ""
	bds, err = NewBucketDataSource(serverURLs, "poolName", bucketName, bucketUUID,
		vbucketIds, authFunc, receiver, options)
	if err == nil || bds != nil {
		t.Errorf("expected err")
	}

	receiver = &TestReceiver{testName: "TestNewBucketDataSource"}
	bds, err = NewBucketDataSource(serverURLs, "poolName", "bucketName", bucketUUID,
		vbucketIds, authFunc, receiver, options)
	if err != nil || bds == nil {
		t.Errorf("expected no err")
	}
}

func TestImmediateStartClose(t *testing.T) {
	connectBucket := func(serverURL, poolName, bucketName, bucketUUID string,
		authFunc AuthFunc) (Bucket, error) {
		return nil, fmt.Errorf("fake connectBucket err")
	}

	connect := func(protocol, dest string) (*memcached.Client, error) {
		if protocol != "tcp" || dest != "serverA" {
			t.Errorf("unexpected connect, protocol: %s, dest: %s", protocol, dest)
		}
		return nil, fmt.Errorf("fake connect err")
	}

	serverURLs := []string{"serverA"}
	bucketUUID := ""
	vbucketIds := []uint16(nil)
	var authFunc AuthFunc
	receiver := &TestReceiver{testName: "TestImmediateStartClose"}
	options := &BucketDataSourceOptions{
		ConnectBucket: connectBucket,
		Connect:       connect,
	}

	bds, err := NewBucketDataSource(serverURLs, "poolName", "bucketName", bucketUUID,
		vbucketIds, authFunc, receiver, options)
	if err != nil || bds == nil {
		t.Errorf("expected no err, got err: %v", err)
	}

	err = bds.Close()
	if err == nil {
		t.Errorf("expected err on Close before Start")
	}
	err = bds.Start()
	if err != nil {
		t.Errorf("expected no err on Start")
	}
	err = bds.Start()
	if err == nil {
		t.Errorf("expected err on re-Start")
	}
	err = bds.Close()
	if err != nil {
		t.Errorf("expected no err on Close")
	}
	err = bds.Close()
	if err == nil {
		t.Errorf("expected err on Close")
	}
}

func TestBucketDataSourceStartNilVBSM(t *testing.T) {
	var connectBucketResult Bucket
	var connectBucketErr error
	var connectBucketCh chan []string
	var connectCh chan []string

	connectBucket := func(serverURL, poolName,
		bucketName, bucketUUID string, authFunc AuthFunc) (Bucket, error) {
		connectBucketCh <- []string{serverURL, poolName, bucketName, bucketUUID}
		return connectBucketResult, connectBucketErr
	}

	connect := func(protocol, dest string) (*memcached.Client, error) {
		if protocol != "tcp" || dest != "serverA" {
			t.Errorf("unexpected connect, protocol: %s, dest: %s", protocol, dest)
		}
		connectCh <- []string{protocol, dest}
		return nil, fmt.Errorf("fake connect err")
	}

	serverURLs := []string{"serverA"}
	bucketUUID := ""
	vbucketIds := []uint16(nil)
	var authFunc AuthFunc
	receiver := &TestReceiver{testName: "TestNewBucketDataSource"}
	options := &BucketDataSourceOptions{
		ConnectBucket: connectBucket,
		Connect:       connect,
	}

	connectBucketResult = &TestBucket{
		uuid: bucketUUID,
		vbsm: nil,
	}
	connectBucketErr = nil
	connectBucketCh = make(chan []string)

	bds, err := NewBucketDataSource(serverURLs, "poolName", "bucketName", bucketUUID,
		vbucketIds, authFunc, receiver, options)
	if err != nil || bds == nil {
		t.Errorf("expected no err, got err: %v", err)
	}
	err = bds.Start()
	if err != nil {
		t.Errorf("expected no-err on Start()")
	}
	c := <-connectBucketCh
	if !reflect.DeepEqual(c, []string{"serverA", "poolName", "bucketName", ""}) {
		t.Errorf("expected connectBucket params")
	}
	select {
	case c := <-connectCh:
		t.Errorf("expected no connect due to nil vbsm, got: %#v", c)
	default:
	}
	err = bds.Close()
	if err != nil {
		t.Errorf("expected clean Close(), got err: %v", err)
	}
	if len(receiver.errs) != 1 {
		t.Errorf("expected connect err")
	}
}

func TestConnectError(t *testing.T) {
	var connectBucketResult Bucket
	var connectBucketErr error
	var connectBucketCh chan []string
	var connectCh chan []string

	connectBucket := func(serverURL, poolName,
		bucketName, bucketUUID string, authFunc AuthFunc) (Bucket, error) {
		connectBucketCh <- []string{serverURL, poolName, bucketName, bucketUUID}
		return connectBucketResult, connectBucketErr
	}

	connect := func(protocol, dest string) (*memcached.Client, error) {
		if protocol != "tcp" || dest != "serverA" {
			t.Errorf("unexpected connect, protocol: %s, dest: %s", protocol, dest)
		}
		connectCh <- []string{protocol, dest}
		return nil, fmt.Errorf("fake-connect-error, protocol: %s, dest: %s",
			protocol, dest)
	}

	serverURLs := []string{"serverA"}
	bucketUUID := ""
	vbucketIds := []uint16{0, 1, 2, 3}
	var authFunc AuthFunc
	receiver := &TestReceiver{testName: "TestBucketDataSourceStartVBSM"}
	options := &BucketDataSourceOptions{
		ConnectBucket: connectBucket,
		Connect:       connect,
	}

	connectBucketResult = &TestBucket{
		uuid: bucketUUID,
		vbsm: &couchbase.VBucketServerMap{
			ServerList: []string{"serverA"},
			VBucketMap: [][]int{
				[]int{0},
				[]int{0},
				[]int{0},
				[]int{0},
			},
		},
	}
	connectBucketErr = nil
	connectBucketCh = make(chan []string)
	connectCh = make(chan []string)

	bds, err := NewBucketDataSource(serverURLs, "poolName", "bucketName", bucketUUID,
		vbucketIds, authFunc, receiver, options)
	if err != nil || bds == nil {
		t.Errorf("expected no err, got err: %v", err)
	}
	err = bds.Start()
	if err != nil {
		t.Errorf("expected no-err on Start()")
	}
	c := <-connectBucketCh
	if !reflect.DeepEqual(c, []string{"serverA", "poolName", "bucketName", ""}) {
		t.Errorf("expected connectBucket params, got: %#v", c)
	}
	c = <-connectCh
	if !reflect.DeepEqual(c, []string{"tcp", "serverA"}) {
		t.Errorf("expected connect params, got: %#v", c)
	}
	err = bds.Close()
	if err != nil {
		t.Errorf("expected clean Close(), got err: %v", err)
	}
	if len(receiver.errs) != 1 {
		t.Errorf("expected connect err")
	}
}

func TestConnThatAlwaysErrors(t *testing.T) {
	var lastRWC *TestRWC

	newFakeConn := func(dest string) io.ReadWriteCloser {
		lastRWC = &TestRWC{name: dest}
		return lastRWC
	}

	var connectBucketResult Bucket
	var connectBucketErr error
	var connectBucketCh chan []string
	var connectCh chan []string

	connectBucket := func(serverURL, poolName,
		bucketName, bucketUUID string, authFunc AuthFunc) (Bucket, error) {
		connectBucketCh <- []string{serverURL, poolName, bucketName, bucketUUID}
		return connectBucketResult, connectBucketErr
	}

	connect := func(protocol, dest string) (*memcached.Client, error) {
		if protocol != "tcp" || dest != "serverA" {
			t.Errorf("unexpected connect, protocol: %s, dest: %s", protocol, dest)
		}
		connectCh <- []string{protocol, dest}
		return memcached.Wrap(newFakeConn(dest))
	}

	serverURLs := []string{"serverA"}
	bucketUUID := ""
	vbucketIds := []uint16{0, 1, 2, 3}
	var authFunc AuthFunc
	receiver := &TestReceiver{testName: "TestBucketDataSourceStartVBSM"}
	options := &BucketDataSourceOptions{
		ConnectBucket: connectBucket,
		Connect:       connect,
	}

	connectBucketResult = &TestBucket{
		uuid: bucketUUID,
		vbsm: &couchbase.VBucketServerMap{
			ServerList: []string{"serverA"},
			VBucketMap: [][]int{
				[]int{0},
				[]int{0},
				[]int{0},
				[]int{0},
			},
		},
	}
	connectBucketErr = nil
	connectBucketCh = make(chan []string)
	connectCh = make(chan []string)

	bds, err := NewBucketDataSource(serverURLs, "poolName", "bucketName", bucketUUID,
		vbucketIds, authFunc, receiver, options)
	if err != nil || bds == nil {
		t.Errorf("expected no err, got err: %v", err)
	}
	err = bds.Start()
	if err != nil {
		t.Errorf("expected no-err on Start()")
	}
	c := <-connectBucketCh
	if !reflect.DeepEqual(c, []string{"serverA", "poolName", "bucketName", ""}) {
		t.Errorf("expected connectBucket params, got: %#v", c)
	}
	c = <-connectCh
	if !reflect.DeepEqual(c, []string{"tcp", "serverA"}) {
		t.Errorf("expected connect params, got: %#v", c)
	}
	err = bds.Close()
	if err != nil {
		t.Errorf("expected clean Close(), got err: %v", err)
	}
	if len(receiver.errs) != 1 {
		t.Errorf("expected connect err")
	}
	if lastRWC == nil {
		t.Errorf("expected a lastRWC")
	}
	if lastRWC.numReads != 0 {
		t.Errorf("expected a lastRWC with 0 reads, %#v", lastRWC)
	}
	if lastRWC.numWrites != 1 {
		t.Errorf("expected a lastRWC with 1 write, %#v", lastRWC)
	}
	if receiver.numSetMetaDatas != 0 {
		t.Errorf("expected 1 set-meta-data, %#v", receiver)
	}
	if receiver.numGetMetaDatas != 0 {
		t.Errorf("expected 1 get-meta-data, %#v", receiver)
	}
}

func TestUPROpen(t *testing.T) {
	var lastRWC *TestRWC

	newFakeConn := func(dest string) io.ReadWriteCloser {
		lastRWC = &TestRWC{
			name: dest,
			readCh: make(chan RWReq),
			writeCh: make(chan RWReq),
		}
		return lastRWC
	}

	var connectBucketResult Bucket
	var connectBucketErr error
	var connectBucketCh chan []string
	var connectCh chan []string

	connectBucket := func(serverURL, poolName,
		bucketName, bucketUUID string, authFunc AuthFunc) (Bucket, error) {
		connectBucketCh <- []string{serverURL, poolName, bucketName, bucketUUID}
		return connectBucketResult, connectBucketErr
	}

	connect := func(protocol, dest string) (*memcached.Client, error) {
		if protocol != "tcp" || dest != "serverA" {
			t.Errorf("unexpected connect, protocol: %s, dest: %s", protocol, dest)
		}
		connectCh <- []string{protocol, dest}
		return memcached.Wrap(newFakeConn(dest))
	}

	serverURLs := []string{"serverA"}
	bucketUUID := ""
	vbucketIds := []uint16{0, 1, 2, 3}
	var authFunc AuthFunc
	receiver := &TestReceiver{testName: "TestBucketDataSourceStartVBSM"}
	options := &BucketDataSourceOptions{
		ConnectBucket: connectBucket,
		Connect:       connect,
	}

	connectBucketResult = &TestBucket{
		uuid: bucketUUID,
		vbsm: &couchbase.VBucketServerMap{
			ServerList: []string{"serverA"},
			VBucketMap: [][]int{
				[]int{0},
				[]int{0},
				[]int{0},
				[]int{0},
			},
		},
	}
	connectBucketErr = nil
	connectBucketCh = make(chan []string)
	connectCh = make(chan []string)

	bds, err := NewBucketDataSource(serverURLs, "poolName", "bucketName", bucketUUID,
		vbucketIds, authFunc, receiver, options)
	if err != nil || bds == nil {
		t.Errorf("expected no err, got err: %v", err)
	}
	err = bds.Start()
	if err != nil {
		t.Errorf("expected no-err on Start()")
	}
	c := <-connectBucketCh
	if !reflect.DeepEqual(c, []string{"serverA", "poolName", "bucketName", ""}) {
		t.Errorf("expected connectBucket params, got: %#v", c)
	}
	c = <-connectCh
	if !reflect.DeepEqual(c, []string{"tcp", "serverA"}) {
		t.Errorf("expected connect params, got: %#v", c)
	}
	rwc := lastRWC
	if rwc == nil {
		t.Errorf("expected a rwc")
	}

	reqW := <- rwc.writeCh
	req := &gomemcached.MCRequest{}
	n, err := req.Receive(bytes.NewReader(reqW.buf), nil)
	if err != nil || n < 24 {
		t.Errorf("expected read req to work, err: %v", err)
	}
	reqW.resCh <- RWRes{n: len(reqW.buf), err: nil}

	res := &gomemcached.MCResponse{
		Opcode: req.Opcode,
		Opaque: req.Opaque,
	}
	reqR := <- rwc.readCh
	copy(reqR.buf, res.HeaderBytes())
	reqR.resCh <- RWRes{n: len(reqR.buf), err: nil}

	err = bds.Close()
	if err != nil {
		t.Errorf("expected clean Close(), got err: %v", err)
	}
	if len(receiver.errs) != 0 {
		t.Errorf("expected 0 errs")
	}
}
