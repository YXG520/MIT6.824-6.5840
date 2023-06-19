package kvraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"MIT6.824-6.5840/labgob"
	"MIT6.824-6.5840/labrpc"
	"MIT6.824-6.5840/raft"
)

type OpId struct {
	ClientId    int64
	SequenceNum int64
}
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType OpType
	Key    string
	Value  string
	Err    Err
	OpId   OpId
}

func (op Op) String() string {
	return fmt.Sprintf("opType=%v clientId=%v sequenceNum=%v key=%v value=%v", op.OpType, op.OpId.ClientId, op.OpId.SequenceNum, op.Key, op.Value)
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	taskCh      map[OpId]chan<- Op
	raftTimeout time.Duration

	clientLastPutAppend map[int64]Op

	KVDB map[string]string

	persister   *raft.Persister
	lastApplied int
}

func (kv *KVServer) String() string {
	return fmt.Sprintf("[KVserver %v]", kv.me)
}

func (kv *KVServer) RegisterTask(op Op, ch chan<- Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf(0, "%v: RegisterTask acquire lock", kv)
	defer DPrintf(0, "%v: RegisterTask release lock", kv)
	DPrintf(100, "%v: RegisterTask %v", kv, op)
	if kv.killed() {
		op.Err = ErrKilled
		ch <- op
		return
	}
	if _, exist := kv.taskCh[op.OpId]; exist {
		op.Err = ErrDuplicate
		ch <- op
		return
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		op.Err = ErrWrongLeader
		ch <- op
		return
	}
	DPrintf(11, "%v: start raft op:%v", kv, op)
	// 将这个可能为空也可能不为空的chan放入到task map中
	// 如果在register步骤就捕捉到了错误，ch不空，会直接返回客户端错误
	// 如果ch为空，在随后的select中遭遇阻塞，，则需要等待ListenApplyMsg中往ch填充消息才能顺利返回
	kv.taskCh[op.OpId] = ch
}

func (kv *KVServer) UnregisterTask(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf(0, "%v: UnregisterTask acquire lock", kv)
	defer DPrintf(0, "%v: UnregisterTask release lock", kv)
	DPrintf(11, "%v: UnregisterTask op=%v", kv, op)
	delete(kv.taskCh, op.OpId)
}
func (kv *KVServer) EncodeSnapshot() []byte {
	DPrintf(12, "%v: send snapshot", kv)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.KVDB)
	e.Encode(kv.clientLastPutAppend)
	data := w.Bytes()
	return data
}
func (kv *KVServer) LoadSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	DPrintf(12, "%v: load snapshot", kv)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.KVDB) != nil ||
		d.Decode(&kv.clientLastPutAppend) != nil {
		//   error...
		errInfo := fmt.Sprintf("%v: LoadKVDB decode error\n", kv)
		panic(errInfo)
	}
	DPrintf(100, "%v: loadKVDB %v", kv, kv.KVDB)
}
func (kv *KVServer) ListenApplyMsg() {
	for msg := range kv.applyCh {
		//TODO
		kv.mu.Lock()
		DPrintf(0, "%v: ListenApplyMsg acquire lock", kv)
		if msg.CommandValid {
			op := msg.Command.(Op)
			DPrintf(11, "%v: come msg CommandIndex=%v op= {%v}", kv, msg.CommandIndex, op)
			if kv.lastApplied != 0 && msg.CommandIndex != kv.lastApplied+1 {
				errinfo := fmt.Sprintf("%v: msg.CommandIndex(%v)!=1+kv.lastApplied(%v) ", kv, msg.CommandIndex, kv.lastApplied)
				panic(errinfo)
			}
			kv.lastApplied = msg.CommandIndex
			switch op.OpType {
			case GetOp:
				op.Err = OK
				op.Value = kv.KVDB[op.Key]

			case PutOp, AppendOp:
				op.Err = OK
				// 为什么需要做两次判断是否重复？
				if lastOp, exist := kv.clientLastPutAppend[op.OpId.ClientId]; exist && lastOp.OpId.SequenceNum >= op.OpId.SequenceNum {
					op.Err = lastOp.Err
					break
				}

				if op.OpType == PutOp {
					kv.KVDB[op.Key] = op.Value
					DPrintf(11, "%v :apply put to KVDB key=%v value=%v", kv, op.Key, kv.KVDB[op.Key])
				} else {
					kv.KVDB[op.Key] += op.Value
					DPrintf(11, "%v :apply append to KVDB key=%v value=%v", kv, op.Key, kv.KVDB[op.Key])
				}
				kv.clientLastPutAppend[op.OpId.ClientId] = op
			default:
				errInfo := fmt.Sprintf("switch op.OpType is wrong")
				panic(errInfo)
			}
			if _, isLeader := kv.rf.GetState(); isLeader {
				if _, exist := kv.taskCh[op.OpId]; exist {
					DPrintf(11, "%v: next apply to client opType=%v, clientId=%v, sequenceNum=%v, key=%v value=%v", kv, op.OpType, op.OpId.ClientId, op.OpId.SequenceNum, op.Key, op.Value)
					kv.taskCh[op.OpId] <- op
					delete(kv.taskCh, op.OpId)
				}
			} else {
				//非leader节点需要清空任务管道 clear taskCh
				for opid, ch := range kv.taskCh {
					op := Op{Err: ErrWrongLeader}
					DPrintf(100, "%v: next talk client I am not leader opid:%v", kv, opid)
					ch <- op
					DPrintf(100, "%v: finish talk client I am not leader opid:%v", kv, opid)
				}
				kv.taskCh = make(map[OpId]chan<- Op)
			}

			if kv.maxraftstate > 0 && float64(kv.persister.RaftStateSize()) > float64(kv.maxraftstate)*0.9 {
				index := msg.CommandIndex
				data := kv.EncodeSnapshot()
				kv.rf.Snapshot(index, data)
			}
		} else if msg.SnapshotValid {
			DPrintf(12, "%v: come snapshot SnapshotIndex=%v kv.lastApplied=%v", kv, msg.SnapshotIndex, kv.lastApplied)
			// data := msg.Snapshot
			// kv.LoadKVDB(data)
			if msg.SnapshotIndex > kv.lastApplied {
				data := msg.Snapshot
				kv.LoadSnapshot(data)
				kv.lastApplied = msg.SnapshotIndex
			} else {
				panic("7777")
			}
		} else {
			errInfo := fmt.Sprintf("%v: msg.CommandValid=%v msg.SnapshotValid=%v both is false", kv, msg.CommandValid, msg.SnapshotValid)
			panic(errInfo)
		}
		DPrintf(0, "%v: ListenApplyMsg release lock", kv)
		kv.mu.Unlock()
		if kv.killed() {
			return
		}
	}
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrKilled
		return
	}
	DPrintf(11, "%v: Get args.ClientId=%v args.SequenceNum=%v args.Key=%v start", kv, args.ClientId, args.SequenceNum, args.Key)
	op := Op{
		OpType: GetOp,
		Key:    args.Key,
		OpId:   OpId{args.ClientId, args.SequenceNum},
	}
	ch := make(chan Op, 1)
	kv.RegisterTask(op, ch)
	defer kv.UnregisterTask(op)
	timer := time.NewTimer(kv.raftTimeout)
	select {
	case retOp := <-ch:
		reply.Err = retOp.Err
		reply.Value = retOp.Value
	case <-timer.C:
		reply.Err = ErrTimeout
	}
	DPrintf(11, "%v: args.OpType=%v args.ClientId=%v args.SequenceNum=%v args.Key=%v reply.value=%v reply.Err=%v end", kv, GetOp, args.ClientId, args.SequenceNum, args.Key, reply.Value, reply.Err)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrKilled
		return
	}
	DPrintf(11, "%v: PutAppend args.OpType=%v args.ClientId=%v args.SequenceNum=%v args.Key=%v args.value=%v start", kv, args.OpType, args.ClientId, args.SequenceNum, args.Key, args.Value)
	op := Op{
		OpType: args.OpType,
		Key:    args.Key,
		Value:  args.Value,
		OpId:   OpId{args.ClientId, args.SequenceNum},
	}
	ch := make(chan Op, 1)
	kv.RegisterTask(op, ch)
	defer kv.UnregisterTask(op)
	timer := time.NewTimer(kv.raftTimeout)
	select {
	case retOp := <-ch:
		reply.Err = retOp.Err
	case <-timer.C:
		reply.Err = ErrTimeout
	}
	DPrintf(11, "%v: PutAppend args.OpType=%v args.ClientId=%v args.SequenceNum=%v args.Key=%v reply.Err=%v end", kv, args.OpType, args.ClientId, args.SequenceNum, args.Key, reply.Err)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	DPrintf(11, "%v: is killed", kv)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister
	// You may need initialization code here.
	kv.taskCh = make(map[OpId]chan<- Op)
	kv.raftTimeout = 2 * time.Second

	kv.clientLastPutAppend = make(map[int64]Op)

	kv.lastApplied = 0
	kv.KVDB = make(map[string]string)
	kv.LoadSnapshot(kv.persister.ReadSnapshot())
	go kv.ListenApplyMsg()

	testTimer := time.NewTimer(time.Second * 5)
	go kv.alive(testTimer)
	go func() {
		<-testTimer.C
		if kv.killed() {
			return
		}
		tmpInfo := fmt.Sprintf("%v: bomb", kv)
		panic(tmpInfo)
	}()
	return kv
}
func (kv *KVServer) alive(testTimer *time.Timer) {
	for !kv.killed() {
		DPrintf(100, "%v: alive wait acquire lock", kv)
		kv.mu.Lock()
		testTimer.Reset(time.Second * 5)
		DPrintf(100, "%v: I am alive\n", kv)
		kv.mu.Unlock()
		time.Sleep(time.Second)
	}
	testTimer.Stop()
}
