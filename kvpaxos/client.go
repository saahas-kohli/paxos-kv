package kvpaxos

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net/rpc"
	"time"
)

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	lastReqID  int64  
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastReqID = -1
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// retry helper: keeps sending RPCs until one succeeds
func (ck *Clerk) retryRPC(rpcName string, args interface{}, reply interface{}) {
	for {
		for _, srv := range ck.servers {
			if call(srv, rpcName, args, reply) {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Get fetches the value for the given key, retrying until success
func (ck *Clerk) Get(key string) string {
	seq := nrand()
	args := &GetArgs{
		Key: key,
		IDs: IDPair{Seq: seq, Clean: ck.lastReqID},
	}
	var reply GetReply

	ck.retryRPC("KVPaxos.Get", args, &reply)
	ck.lastReqID = seq
	return reply.Value
}

// PutAppend sends a Put or Append request, retrying until acknowledged
func (ck *Clerk) PutAppend(key, value, op string) {
	seq := nrand()
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		IDs:   IDPair{Seq: seq, Clean: ck.lastReqID},
	}
	var reply PutAppendReply

	ck.retryRPC("KVPaxos.PutAppend", args, &reply)
	ck.lastReqID = seq
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
