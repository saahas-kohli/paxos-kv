package paxos

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)

type Fate int

const (
	Decided   Fate = iota + 1
	Pending
	Forgotten
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32
	unreliable int32
	rpcCount   int32
	peers      []string
	me         int

	doneSeqs    []int
	promiseMap  map[int]int64
	acceptMap   map[int]int64
	valMap      map[int]interface{}
	decisionMap map[int]bool
}

// RPCArgs is a unified RPC argument type for all phases.
type RPCArgs struct {
	Phase    int        
	Instance int         
	Number   int64       
	Payload  interface{} 
	From     int        
}

// RPCReply carries all possible reply fields.
type RPCReply struct {
	Code          int         
	PromiseHigh   int64      
	AcceptHigh    int64     
	Payload       interface{} 
}

// Make constructs a new Paxos peer.
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{
		peers:       peers,
		me:          me,
		doneSeqs:    make([]int, len(peers)),
		promiseMap:  make(map[int]int64),
		acceptMap:   make(map[int]int64),
		valMap:      make(map[int]interface{}),
		decisionMap: make(map[int]bool),
	}
	for i := range px.doneSeqs {
		px.doneSeqs[i] = -1
	}
	if rpcs != nil {
		rpcs.Register(px)
	} else {
		srv := rpc.NewServer()
		srv.Register(px)
		os.Remove(peers[me])
		ln, err := net.Listen("unix", peers[me])
		if err != nil {
			log.Fatal("listen error:", err)
		}
		px.l = ln
		go px.acceptLoop(srv)
	}
	return px
}

func (px *Paxos) acceptLoop(srv *rpc.Server) {
	for !px.isDead() {
		conn, err := px.l.Accept()
		if err != nil {
			if px.isDead() {
				return
			}
			continue
		}
		if px.isUnreliable() {
			r := rand.Intn(1000)
			if r < 100 {
				conn.Close()
				continue
			}
			if r < 300 {
				uconn := conn.(*net.UnixConn)
				f, _ := uconn.File()
				syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
			}
		}
		atomic.AddInt32(&px.rpcCount, 1)
		go srv.ServeConn(conn)
	}
}

func call(svr, method string, args, reply interface{}) bool {
	c, err := rpc.Dial("unix", svr)
	if err != nil {
		if oe, ok := err.(*net.OpError); ok {
			if oe.Err != syscall.ENOENT && oe.Err != syscall.ECONNREFUSED {
				fmt.Printf("Dial error: %v\n", oe)
			}
		}
		return false
	}
	defer c.Close()
	if err := c.Call(method, args, reply); err != nil {
		return false
	}
	return true
}

func (px *Paxos) isDead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

func (px *Paxos) setunreliable(u bool) {
	if u {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isUnreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

// ensureEntry initializes maps for a new instance if absent.
func (px *Paxos) ensureEntry(inst int) {
	if _, exists := px.promiseMap[inst]; !exists {
		px.promiseMap[inst] = -1
		px.acceptMap[inst] = -1
		px.valMap[inst] = nil
		px.decisionMap[inst] = false
	}
}

// MessageHandler handles all Paxos RPCs with numeric codes.
func (px *Paxos) MessageHandler(a *RPCArgs, r *RPCReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	inst := a.Instance
	px.ensureEntry(inst)
	pm, am, vm, dm := px.promiseMap, px.acceptMap, px.valMap, px.decisionMap

	switch a.Phase {
	case 0:
		if a.Number <= pm[inst] {
			r.Code = 1
			return nil
		}
		pm[inst] = a.Number
		r.Code = 0
		r.PromiseHigh = a.Number
		r.AcceptHigh = am[inst]
		r.Payload = vm[inst]

	case 1:
		if a.Number < pm[inst] {
			r.Code = 1
			return nil
		}
		pm[inst], am[inst], vm[inst] = a.Number, a.Number, a.Payload
		r.Code = 0

	case 2:
		vm[inst], dm[inst] = a.Payload, true
		r.Code = 0

	case 3:
		sender := a.From
		if a.Instance > px.doneSeqs[sender] {
			px.doneSeqs[sender] = a.Instance
			limit := px.Min()
			for i := 0; i < limit; i++ {
				if dm[i] {
					delete(pm, i)
					delete(am, i)
					delete(vm, i)
					delete(dm, i)
				}
			}
		}
		r.Code = 0
	}
	return nil
}

// Start begins consensus on instance seq with value v.
func (px *Paxos) Start(seq int, v interface{}) {
	go px.propose(seq, v)
}

func (px *Paxos) sendInit(seq int, propID int64, current interface{}) (int, int64, interface{}) {
	cnt, mx, best := 0, int64(-1), current
	for idx, peer := range px.peers {
		arg := &RPCArgs{0, seq, propID, nil, 0}
		rep := &RPCReply{}
		if idx == px.me {
			px.MessageHandler(arg, rep)
		} else if !call(peer, "Paxos.MessageHandler", arg, rep) {
			rep.Code = 2
		}
		if rep.Code == 0 {
			cnt++
			if rep.AcceptHigh > mx {
				mx, best = rep.AcceptHigh, rep.Payload
			}
		}
	}
	return cnt, mx, best
}

// sendAccept sends accept requests; returns count of OKs.
func (px *Paxos) sendAccept(seq int, propID int64, val interface{}) int {
	cnt := 0
	for idx, peer := range px.peers {
		arg := &RPCArgs{1, seq, propID, val, 0}
		rep := &RPCReply{}
		if idx == px.me {
			px.MessageHandler(arg, rep)
		} else if !call(peer, "Paxos.MessageHandler", arg, rep) {
			rep.Code = 2
		}
		if rep.Code == 0 {
			cnt++
		}
	}
	return cnt
}

// broadcastLearn informs all peers of learned value.
func (px *Paxos) broadcastLearn(seq int, val interface{}) {
	for idx, peer := range px.peers {
		arg := &RPCArgs{2, seq, 0, val, 0}
		rep := &RPCReply{}
		if idx == px.me {
			px.MessageHandler(arg, rep)
		} else {
			call(peer, "Paxos.MessageHandler", arg, rep)
		}
	}
}

// propose runs the Paxos proposer using numeric ops and a bitwise init for propID.
func (px *Paxos) propose(seq int, v interface{}) {
	px.mu.Lock()
	px.ensureEntry(seq)
	px.mu.Unlock()

	// unique ID: start with peer ID in low bits, set the 33rd bit
	propID := int64(px.me)
	propID |= (1 << 32)
	chosenVal := v
	_ = chosenVal
	bestN := int64(-1)
	_ = bestN
	majority := len(px.peers)/2 + 1

loop:
	for {
		if px.isDead() {
			break
		}
		propID++ // bump

		// Phase 1
		ok, mx, val := px.sendInit(seq, propID, chosenVal)
		if ok < majority {
			continue loop
		}
		bestN, chosenVal = mx, val

		// Phase 2
		if px.sendAccept(seq, propID, chosenVal) < majority {
			continue loop
		}

		// Phase 3
		px.broadcastLearn(seq, chosenVal)
		break
	}
}

// Done notifies others this peer is done up through seq.
func (px *Paxos) Done(seq int) {
	if seq <= px.doneSeqs[px.me] {
		return
	}
	for idx, peer := range px.peers {
		arg := &RPCArgs{3, seq, 0, nil, px.me}
		rep := &RPCReply{}
		if idx == px.me {
			px.MessageHandler(arg, rep)
		} else {
			call(peer, "Paxos.MessageHandler", arg, rep)
		}
	}
}

// Max returns the highest-known sequence instance.
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	h := -1
	for s := range px.promiseMap {
		if s > h {
			h = s
		}
	}
	return h
}

// Min returns one past the minimum Done() across all peers.
func (px *Paxos) Min() int {
	mn := px.doneSeqs[0]
	for _, v := range px.doneSeqs {
		if v < mn {
			mn = v
		}
	}
	return mn + 1
}

// Status inspects local state for seq.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	if seq < px.Min() {
		return Forgotten, nil
	}
	px.mu.Lock()
	defer px.mu.Unlock()
	if px.decisionMap[seq] {
		return Decided, px.valMap[seq]
	}
	return Pending, nil
}
