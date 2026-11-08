package kvpaxos

import (
	"cs134-25s-kv/paxos"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		//log.Printf(format, a...)
	}
	return
}

type KVPair struct {
	Key   string
	Value string
}

type Op struct {
	Payload KVPair
	OpType  string
	IDs     IDPair
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	nextIdx   int
	store     map[string]string
	doneCache map[int64]string
}

// helper for map writes
func mkEntry(m map[string]string, k, v string) { m[k] = v }

// helper for map reads
func rdEntry(m map[string]string, k string) (string, bool) {
	v, ok := m[k]
	return v, ok
}

func waitDec(kv *KVPaxos, id int, val Op) Op {
	backoff := 10 * time.Millisecond
	for {
		st, v := kv.px.Status(id)
		if st == paxos.Decided {
			return v.(Op)
		}
		time.Sleep(backoff)
		if backoff < 10*time.Second {
			backoff *= 2
		}
	}
}

func (kv *KVPaxos) isDupGet(args *GetArgs) bool {
	if prev, ok := kv.doneCache[args.IDs.Seq]; ok {
		return prev == kv.store[args.Key]
	}
	return false
}

func (kv *KVPaxos) isDupPA(args *PutAppendArgs) bool {
	_, ok := kv.doneCache[args.IDs.Seq]
	return ok
}

func (kv *KVPaxos) runInstance(idx int, op Op) Op {
	kv.px.Start(idx, op)
	return waitDec(kv, idx, op)
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.isDupGet(args) {
		reply.Value = kv.store[args.Key]
		reply.Err = OK
		return nil
	}

	for {
		n := kv.nextIdx
		kv.nextIdx++
		operation := Op{
			Payload: KVPair{Key: args.Key, Value: ""},
			OpType:  "Get",
			IDs:     IDPair{Seq: args.IDs.Seq, Clean: args.IDs.Clean},
		}
		res := kv.runInstance(n, operation)
		op := res
		key := op.Payload.Key
		val := op.Payload.Value
		seq := op.IDs.Seq

		switch op.OpType {
		case "Get":
			if stored, ok := kv.store[key]; ok {
				kv.doneCache[seq] = stored
			} else {
				kv.doneCache[seq] = ErrNoKey
			}
		case "Put":
			mkEntry(kv.store, key, val)
			kv.doneCache[seq] = OK
		case "Append":
			if old, ok := rdEntry(kv.store, key); ok {
				mkEntry(kv.store, key, old+val)
			} else {
				mkEntry(kv.store, key, val)
			}
			kv.doneCache[seq] = OK
		}
		kv.px.Done(n)
		_, exists := kv.doneCache[operation.IDs.Clean]
		if operation.IDs.Clean != -1 && exists {
			delete(kv.doneCache, operation.IDs.Clean)
		}
		if res.IDs.Seq == args.IDs.Seq {
			out := kv.doneCache[args.IDs.Seq]
			if out == ErrNoKey {
				reply.Value, reply.Err = "", ErrNoKey
			} else {
				reply.Value, reply.Err = out, OK
			}
			break
		}
	}
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.isDupPA(args) {
		reply.Err = OK
		return nil
	}

	for {
		n := kv.nextIdx
		kv.nextIdx++
		operation := Op{
			Payload: KVPair{Key: args.Key, Value: args.Value},
			OpType:  args.Op,
			IDs:     IDPair{Seq: args.IDs.Seq, Clean: args.IDs.Clean},
		}
		res := kv.runInstance(n, operation)
		op := res
		key := op.Payload.Key
		val := op.Payload.Value
		seq := op.IDs.Seq

		switch op.OpType {
		case "Get":
			if stored, ok := kv.store[key]; ok {
				kv.doneCache[seq] = stored
			} else {
				kv.doneCache[seq] = ErrNoKey
			}
		case "Put":
			mkEntry(kv.store, key, val)
			kv.doneCache[seq] = OK
		case "Append":
			if old, ok := rdEntry(kv.store, key); ok {
				mkEntry(kv.store, key, old+val)
			} else {
				mkEntry(kv.store, key, val)
			}
			kv.doneCache[seq] = OK
		}
		kv.px.Done(n)
		_, exists := kv.doneCache[operation.IDs.Clean]
		if operation.IDs.Clean != -1 && exists {
			delete(kv.doneCache, operation.IDs.Clean)
		}
		if res.IDs.Seq == args.IDs.Seq {
			break
		}
	}
	reply.Err = OK
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

// StartServer unchanged except store and doneCache initialization
func StartServer(servers []string, me int) *KVPaxos {
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	kv.store = make(map[string]string)
	kv.nextIdx = 0
	kv.doneCache = make(map[int64]string)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)
	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && rand.Int63()%1000 < 100 {
					conn.Close()
				} else if kv.isunreliable() && rand.Int63()%1000 < 200 {
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
