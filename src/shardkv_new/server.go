package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"sync"

	"MIT6.824-6.5840/labgob"
	"MIT6.824-6.5840/labrpc"
	"MIT6.824-6.5840/raft"
	"MIT6.824-6.5840/shardctrler"
)

//----------------------------------------------------结构体定义部分------------------------------------------------------

const (
	UpConfigLoopInterval = 100 * time.Millisecond // poll configuration period

	GetTimeout          = 500 * time.Millisecond
	AppOrPutTimeout     = 500 * time.Millisecond
	UpConfigTimeout     = 500 * time.Millisecond
	AddShardsTimeout    = 500 * time.Millisecond
	RemoveShardsTimeout = 500 * time.Millisecond
)

type Shard struct {
	KvMap     map[string]string
	ConfigNum int // what version this Shard is in
}

type Op struct {

	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqId    int
	OpType   Operation // "get" "put" "append"
	Key      string
	Value    string
	UpConfig shardctrler.Config
	ShardId  int
	Shard    Shard
	SeqMap   map[int64]int
}

// OpReply is used to wake waiting RPC caller after Op arrived from applyCh
type OpReply struct {
	ClientId int64
	SeqId    int
	Err      Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxRaftState int // snapshot if log grows this big

	// Your definitions here.

	dead int32 // set by Kill()

	Config     shardctrler.Config // 需要更新的最新的配置
	LastConfig shardctrler.Config // 更新之前的配置，用于比对是否全部更新完了

	shardsPersist []Shard // ShardId -> Shard 如果KvMap == nil则说明当前的数据不归当前分片管
	waitChMap     map[int]chan OpReply
	SeqMap        map[int64]int
	sck           *shardctrler.Clerk // sck is a client used to contact shard master
}

//-------------------------------------------------初始化(Start)部分------------------------------------------------------

// StartServer me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxRaftState bytes, in order to allow Raft to garbage-collect its
// log. if maxRaftState is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass masters[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// UpConfig.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific Shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxRaftState = maxraftstate
	kv.makeEnd = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	kv.shardsPersist = make([]Shard, shardctrler.NShards)

	kv.SeqMap = make(map[int64]int)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.masters)
	kv.sck = shardctrler.MakeClerk(kv.masters)
	kv.waitChMap = make(map[int]chan OpReply)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyMsgHandlerLoop()
	go kv.ConfigDetectedLoop()

	return kv
}

//------------------------------------------------------Loop部分--------------------------------------------------------

// applyMsgHandlerLoop 处理applyCh发送过来的ApplyMsg
func (kv *ShardKV) applyMsgHandlerLoop() {
	for {
		if kv.killed() {
			return
		}
		select {

		case msg := <-kv.applyCh:

			if msg.CommandValid == true {
				kv.mu.Lock()
				op := msg.Command.(Op)
				reply := OpReply{
					ClientId: op.ClientId,
					SeqId:    op.SeqId,
					Err:      OK,
				}

				if op.OpType == PutType || op.OpType == GetType || op.OpType == AppendType {

					shardId := key2shard(op.Key)

					//
					if kv.Config.Shards[shardId] != kv.gid {
						reply.Err = ErrWrongGroup
					} else if kv.shardsPersist[shardId].KvMap == nil {
						// 如果应该存在的切片没有数据那么这个切片就还没到达
						reply.Err = ShardNotArrived
					} else {

						if !kv.ifDuplicate(op.ClientId, op.SeqId) {

							kv.SeqMap[op.ClientId] = op.SeqId
							switch op.OpType {
							case PutType:
								kv.shardsPersist[shardId].KvMap[op.Key] = op.Value
							case AppendType:
								kv.shardsPersist[shardId].KvMap[op.Key] += op.Value
							case GetType:
								// 如果是Get都不用做
							default:
								log.Fatalf("invalid command type: %v.", op.OpType)
							}
						}
					}
				} else {
					// request from server of other group
					switch op.OpType {

					case UpConfigType:
						kv.upConfigHandler(op)
					case AddShardType:

						// 如果配置号比op的SeqId还低说明不是最新的配置
						if kv.Config.Num < op.SeqId {
							reply.Err = ConfigNotArrived
							break
						}
						kv.addShardHandler(op)
					case RemoveShardType:
						// remove operation is from previous UpConfig
						kv.removeShardHandler(op)
					default:
						log.Fatalf("invalid command type: %v.", op.OpType)
					}
				}

				// 如果需要snapshot，且超出其stateSize
				if kv.maxRaftState != -1 && kv.rf.GetRaftStateSize() > kv.maxRaftState {
					snapshot := kv.PersistSnapShot()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}

				ch := kv.getWaitCh(msg.CommandIndex)
				ch <- reply
				kv.mu.Unlock()

			}

			if msg.SnapshotValid == true {
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					// 读取快照的数据
					kv.mu.Lock()
					kv.DecodeSnapShot(msg.Snapshot)
					kv.mu.Unlock()
				}
				continue
			}

		}
	}

}
func (kv *ShardKV) sayBasicInfo() string {
	return fmt.Sprintf("[gid:%d, kv.me:%d, kv.latestConfig：%v，\n kv.curConfig:%v, kv.maxraftstate:%d, "+
		"] \n， [kv.shardPersist:%v]", kv.gid, kv.me, kv.LastConfig, kv.Config, kv.maxRaftState, kv.shardsPersist)
}

// ConfigDetectedLoop 配置检测
func (kv *ShardKV) ConfigDetectedLoop() {
	kv.mu.Lock()

	curConfig := kv.Config
	rf := kv.rf
	kv.mu.Unlock()

	for !kv.killed() {
		// only leader needs to deal with configuration tasks
		if _, isLeader := rf.GetState(); !isLeader {
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		kv.mu.Lock()

		// 判断是否把不属于自己的部分给分给别人了
		if !kv.allSent() {
			SeqMap := make(map[int64]int)
			for k, v := range kv.SeqMap {
				SeqMap[k] = v
			}
			for shardId, gid := range kv.LastConfig.Shards {

				// 将最新配置里不属于自己的分片分给别人
				if gid == kv.gid && kv.Config.Shards[shardId] != kv.gid && kv.shardsPersist[shardId].ConfigNum < kv.Config.Num {

					sendDate := kv.cloneShard(kv.Config.Num, kv.shardsPersist[shardId].KvMap)

					args := SendShardArg{
						LastAppliedRequestId: SeqMap,
						ShardId:              shardId,
						Shard:                sendDate,
						ClientId:             int64(gid),
						RequestId:            kv.Config.Num,
					}

					// shardId -> gid -> server names
					serversList := kv.Config.Groups[kv.Config.Shards[shardId]]
					servers := make([]*labrpc.ClientEnd, len(serversList))
					for i, name := range serversList {
						servers[i] = kv.makeEnd(name)
					}

					// 开启协程对每个客户端发送切片(这里发送的应是别的组别，自身的共识组需要raft进行状态修改）
					go func(servers []*labrpc.ClientEnd, args *SendShardArg) {

						index := 0
						start := time.Now()
						for {
							var reply AddShardReply
							// 对自己的共识组内进行add
							ok := servers[index].Call("ShardKV.AddShard", args, &reply)

							// 如果给予切片成功，或者时间超时，这两种情况都需要进行GC掉不属于自己的切片
							if ok && reply.Err == OK || time.Now().Sub(start) >= 2*time.Second {
								DPrintf(111, "%v：成功收到来自复制组%d关于接收切片%d的响应！！准备移除分片", kv.sayBasicInfo(), kv.Config.Shards[shardId], shardId)

								// 如果成功
								kv.mu.Lock()
								command := Op{
									OpType:   RemoveShardType,
									ClientId: int64(kv.gid),
									SeqId:    kv.Config.Num,
									ShardId:  args.ShardId,
								}
								kv.mu.Unlock()
								kv.startCommand(command, RemoveShardsTimeout)
								DPrintf(111, "%v:删除分片%d成功", kv.sayBasicInfo(), args.ShardId)

								break
							}
							index = (index + 1) % len(servers)
							if index == 0 {
								time.Sleep(UpConfigLoopInterval)
							}
						}
					}(servers, &args)
				}
			}
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		if !kv.allReceived() {
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		// current configuration is configured, poll for the next configuration
		curConfig = kv.Config
		sck := kv.sck
		kv.mu.Unlock()

		newConfig := sck.Query(curConfig.Num + 1)
		if newConfig.Num != curConfig.Num+1 {
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		command := Op{
			OpType:   UpConfigType,
			ClientId: int64(kv.gid),
			SeqId:    newConfig.Num,
			UpConfig: newConfig,
		}
		kv.startCommand(command, UpConfigTimeout)
	}

}

//------------------------------------------------------RPC部分----------------------------------------------------------

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived {
		return
	}
	command := Op{
		OpType:   GetType,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		Key:      args.Key,
	}
	err := kv.startCommand(command, GetTimeout)
	if err != OK {
		reply.Err = err
		return
	}
	kv.mu.Lock()

	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	} else {
		reply.Err = OK
		reply.Value = kv.shardsPersist[shardId].KvMap[args.Key]
	}
	kv.mu.Unlock()
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived {
		return
	}
	command := Op{
		OpType:   args.Op,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		Key:      args.Key,
		Value:    args.Value,
	}
	reply.Err = kv.startCommand(command, AppOrPutTimeout)
	return
}

// AddShard move shards from caller to this server
func (kv *ShardKV) AddShard(args *SendShardArg, reply *AddShardReply) {
	command := Op{
		OpType:   AddShardType,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		ShardId:  args.ShardId,
		Shard:    args.Shard,
		SeqMap:   args.LastAppliedRequestId,
	}
	reply.Err = kv.startCommand(command, AddShardsTimeout)
	DPrintf(111, "%v:对请求试图发送切片(ClientId:%d, ShardId:%d)的请求的响应状态：%v, ", kv.sayBasicInfo(), args.ClientId, args.ShardId, reply.Err)

	return
}

//------------------------------------------------------handler部分------------------------------------------------------

// 更新最新的config的handler
func (kv *ShardKV) upConfigHandler(op Op) {
	curConfig := kv.Config
	upConfig := op.UpConfig
	if curConfig.Num >= upConfig.Num {
		return
	}
	for shard, gid := range upConfig.Shards {
		if gid == kv.gid && curConfig.Shards[shard] == 0 {
			// 如果更新的配置的gid与当前的配置的gid一样且分片为0(未分配）
			kv.shardsPersist[shard].KvMap = make(map[string]string)
			kv.shardsPersist[shard].ConfigNum = upConfig.Num
		}
	}
	kv.LastConfig = curConfig
	kv.Config = upConfig

}

func (kv *ShardKV) addShardHandler(op Op) {
	// this shard is added or it is an outdated command
	if kv.shardsPersist[op.ShardId].KvMap != nil || op.Shard.ConfigNum < kv.Config.Num {
		return
	}

	kv.shardsPersist[op.ShardId] = kv.cloneShard(op.Shard.ConfigNum, op.Shard.KvMap)

	for clientId, seqId := range op.SeqMap {
		if r, ok := kv.SeqMap[clientId]; !ok || r < seqId {
			kv.SeqMap[clientId] = seqId
		}
	}
	DPrintf(111, "%v: 成功接收到来自复制组%d分片%d", kv.sayBasicInfo(), op.ClientId, op.ShardId)

}

func (kv *ShardKV) removeShardHandler(op Op) {
	if op.SeqId < kv.Config.Num {
		return
	}
	kv.shardsPersist[op.ShardId].KvMap = nil
	kv.shardsPersist[op.ShardId].ConfigNum = op.SeqId
	DPrintf(111, "%v: 成功移除分片%d", kv.sayBasicInfo(), op.ShardId)
}

//------------------------------------------------------持久化快照部分-----------------------------------------------------

// PersistSnapShot Snapshot get snapshot data of kvserver
func (kv *ShardKV) PersistSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(kv.shardsPersist)
	err = e.Encode(kv.SeqMap)
	err = e.Encode(kv.maxRaftState)
	err = e.Encode(kv.Config)
	err = e.Encode(kv.LastConfig)
	if err != nil {
		log.Fatalf("[%d-%d] fails to take snapshot.", kv.gid, kv.me)
	}
	return w.Bytes()
}

// DecodeSnapShot install a given snapshot
func (kv *ShardKV) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var shardsPersist []Shard
	var SeqMap map[int64]int
	var MaxRaftState int
	var Config, LastConfig shardctrler.Config

	if d.Decode(&shardsPersist) != nil || d.Decode(&SeqMap) != nil ||
		d.Decode(&MaxRaftState) != nil || d.Decode(&Config) != nil || d.Decode(&LastConfig) != nil {
		log.Fatalf("[Server(%v)] Failed to decode snapshot！！！", kv.me)
	} else {
		kv.shardsPersist = shardsPersist
		kv.SeqMap = SeqMap
		kv.maxRaftState = MaxRaftState
		kv.Config = Config
		kv.LastConfig = LastConfig

	}
}

//------------------------------------------------------utils封装部分----------------------------------------------------

// Kill the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 传入op的SeqId此次op
func (kv *ShardKV) ifDuplicate(clientId int64, seqId int) bool {

	lastSeqId, exist := kv.SeqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *ShardKV) getWaitCh(index int) chan OpReply {

	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan OpReply, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

func (kv *ShardKV) allSent() bool {
	for shard, gid := range kv.LastConfig.Shards {
		// 如果当前配置中分片中的信息不匹配，且持久化中的配置号更小，说明还未发送
		if gid == kv.gid && kv.Config.Shards[shard] != kv.gid && kv.shardsPersist[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) allReceived() bool {
	for shard, gid := range kv.LastConfig.Shards {

		// 判断切片是否都收到了
		if gid != kv.gid && kv.Config.Shards[shard] == kv.gid && kv.shardsPersist[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) startCommand(command Op, timeoutPeriod time.Duration) Err {
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}

	ch := kv.getWaitCh(index)
	kv.mu.Unlock()

	timer := time.NewTicker(timeoutPeriod)
	defer timer.Stop()

	select {
	case re := <-ch:
		kv.mu.Lock()
		delete(kv.waitChMap, index)
		if re.SeqId != command.SeqId || re.ClientId != command.ClientId {
			// One way to do this is for the server to detect that it has lost leadership,
			// by noticing that a different request has appeared at the index returned by Start()
			kv.mu.Unlock()
			return ErrInconsistentData
		}
		kv.mu.Unlock()
		return re.Err

	case <-timer.C:
		return ErrOverTime
	}
}

// 判断当前节点是否处于迁移期
func (kv *ShardKV) cloneShard(ConfigNum int, KvMap map[string]string) Shard {

	migrateShard := Shard{
		KvMap:     make(map[string]string),
		ConfigNum: ConfigNum,
	}

	for k, v := range KvMap {
		migrateShard.KvMap[k] = v
	}

	return migrateShard
}
