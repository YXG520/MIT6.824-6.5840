package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"MIT6.824-6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this [Client 536]: MakeClerkstruct.
	prefer      int
	timeout     time.Duration
	mu          sync.Mutex
	clientId    int64
	sequenceNum int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.sequenceNum = 0
	ck.prefer = int(nrand()) % len(ck.servers)
	ck.timeout = 500 * time.Millisecond

	DPrintf(500, "[Client %v]: MakeClerk", ck.clientId)
	return ck
}
func (ck *Clerk) String() string {
	return fmt.Sprintf("[Client %v]", ck.clientId)
}
func (ck *Clerk) call(callOne func(serverId int) (Err, interface{})) interface{} {
	prefer := ck.prefer

	type result struct {
		serverId int
		err      Err
		reply    interface{}
	}

	ch := make(chan result, len(ck.servers))
	timer := time.NewTimer(ck.timeout)
	defer timer.Stop()

	done := atomic.Bool{}
	done.Store(false)

	var res result
	for i := 0; ; i++ {
		serverId := (prefer + i) % len(ck.servers)
		go func() {
			Err, reply := callOne(serverId)
			if done.Load() {
				return
			}
			ch <- result{serverId, Err, reply}
			DPrintf(10, "[Client %v]: end call serverId=%v prefer=%v Err=%v", ck.clientId, serverId, prefer, Err)
		}()
		select {
		case res = <-ch:
			if res.err == OK {
				done.Store(true)
				goto Done
			}
		case <-timer.C:
			timer.Reset(ck.timeout)
		}
	}
Done:
	ck.prefer = res.serverId
	return res.reply
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.sequenceNum++
	sequenceNum := ck.sequenceNum
	args := GetArgs{
		Key:         key,
		ClientId:    ck.clientId,
		SequenceNum: sequenceNum,
	}
	callOne := func(serverId int) (Err, interface{}) {
		reply := GetReply{}
		DPrintf(11, "[Client %v]: call serverId=%v Get ClientId=%v SequenceNum=%v Key=%v", ck.clientId, serverId, args.ClientId, args.SequenceNum, args.Key)
		ck.servers[serverId].Call("KVServer.Get", &args, &reply)
		return reply.Err, reply
	}
	reply := ck.call(callOne).(GetReply)
	var tmp string
	if len(reply.Value) > 20 {
		tmp = reply.Value[:10] + "..." + reply.Value[:10]
	} else {
		tmp = reply.Value
	}
	DPrintf(11, "end Get ClientId=%v SequenceNum=%v Key=%v value=%v", args.ClientId, args.SequenceNum, key, tmp)
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, optype OpType) {
	// You will have to modify this function.
	// DPrintf(11, "start PutAppend op=%v key=%v", optype, key)
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.sequenceNum++
	sequenceNum := ck.sequenceNum
	args := PutAppendArgs{
		Key:         key,
		Value:       value,
		OpType:      optype,
		ClientId:    ck.clientId,
		SequenceNum: sequenceNum,
	}
	callOne := func(serverId int) (Err, interface{}) {
		reply := PutAppendReply{}
		DPrintf(11, "[Client %v]: call serverId=%v Get ClientId=%v SequenceNum=%v Key=%v", ck.clientId, serverId, args.ClientId, args.SequenceNum, args.Key)
		ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		return reply.Err, reply
	}
	reply := ck.call(callOne).(PutAppendReply)
	_ = reply
	DPrintf(11, "end Get ClientId=%v SequenceNum=%v Key=%v", args.ClientId, args.SequenceNum, key)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendOp)
}
