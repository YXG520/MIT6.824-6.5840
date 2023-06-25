package shardkv

import (
	"MIT6.824-6.5840/labrpc"
	"MIT6.824-6.5840/shardctrler"
	"fmt"
	"sync/atomic"
	"time"
)
import "MIT6.824-6.5840/raft"
import "sync"
import "MIT6.824-6.5840/labgob"

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead         int32
	waitChMap    map[int]chan Op
	seqMap       map[int64]int
	mck          *shardctrler.Clerk
	latestConfig shardctrler.Config // 最新的配置
	//oldConfig    shardctrler.Config // 上一次获得的配置

	kvPersist map[string]string // 存储持久化的KV键值对	K / V
	//kvShardMap map[int]map[string]struct{} // 存储分片到数据库关联键的映射，shardId->key map
	SeqId            int
	lastIncludeIndex int             // 最近一次快照的截止的日志索引
	persister        *raft.Persister // 共享raft的持久化地址，方便查找

	Counter int // 分片计数器：计算从获取到最新的配置开始已经收集了多少个分片

	//IsResharding      bool // 是否正在重新分片中
	TranferringShards map[int]struct{}
	TotalDiffShards   int
}

// 更新配置的操作
type ConfigOp struct {
	// joinOp
	Servers map[int][]string // new GID -> servers mappings
	// leaveOp
	GIDs []int
	// moveOp
	Shard int
	GID   int
	// queryOp
	Num               int // desired config number
	Cfg               shardctrler.Config
	Data              map[string]string
	TranferringShards map[int]struct{} // 正在迁移的分片
}
type Op struct {
	// Your data here.
	ClientId int64
	SeqId    int
	Key      string
	Value    string
	Index    int
	OpType   string

	ConfigOp ConfigOp

	Err Err // 操作op发生的错误
}

func (kv *ShardKV) applyMsgHandlerLoop() {
	for {
		if kv.Killed() {
			return
		}
		select {
		case msg := <-kv.applyCh:
			// 如果是命令消息，则应用命令同时响应客户端
			if msg.CommandValid {
				index := msg.CommandIndex
				// 传来的信息快照已经存储了则直接返回

				op := msg.Command.(Op)

				if !kv.ifDuplicate(op.ClientId, op.SeqId) {
					kv.mu.Lock()
					switch op.OpType {
					case GetOp:
						op.Value = kv.kvPersist[op.Key]
					case PutOp:
						kv.kvPersist[op.Key] = op.Value
						//DPrintf(1111, "put后，结果为%v", kv.kvPersist[op.Key])

					case AppendOp:
						kv.kvPersist[op.Key] += op.Value
						//DPrintf(1111, "Append后，结果为%v", kv.kvPersist[op.Key])
					//case SendShardsOp:
					//	DPrintf(111, "%v: 发送分片, clientId:%d", kv.sayBasicInfo(), op.ClientId)
					//kv.moveData(op)
					case HandleReceiveShardsOp:
						kv.HandleReceiveShardsOp(op)
					case UpdateConfigOp:
						kv.updateDBAndConfig(op)
					}
					if op.ClientId != -1 {
						kv.seqMap[op.ClientId] = op.SeqId
					}
					// 如果需要日志的容量达到规定值则需要制作快照并且投递
					if kv.isNeedSnapshot() {
						go kv.makeSnapshot(msg.CommandIndex)
						//go kv.deliverSnapshot()
					}
					kv.mu.Unlock()
				}
				// 将返回的ch返回waitCh
				if _, isLead := kv.rf.GetState(); isLead {
					if op.OpType != SendShardsOp {
						kv.getWaitCh(index) <- op
					} else {
						DPrintf(111, "%v: 因为是SendShardsOp操作，无需通知", kv.sayBasicInfo())
					}
				}

			} else if msg.SnapshotValid {
				// 如果是raft传递上来的快照消息，就应用快照，但是不需要响应客户
				//DPrintf(11111, "节点%d应用快照", kv.me)
				kv.decodeSnapshot(msg.SnapshotIndex, msg.Snapshot)
			}

		}
	}
}

// 判断是否是重复操作的也比较简单,因为我是对seq进行递增，所以直接比大小即可
func (kv *ShardKV) ifDuplicate(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastSeqId, exist := kv.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *ShardKV) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan Op, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

// 接收分片
func (kv *ShardKV) HandleReceiveShardingRPC(args *MoveShardsArgs, reply *MoveShardsReply) {

	if kv.Killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// 通过配置号过滤掉过期的分片推送
	if args.Cfg.Num < kv.latestConfig.Num {
		reply.Err = ErrStaleConfig
		return
	}

	// 如果配置版本一样，则可以确定自己这些分片一定是自己管理的

	// 封装Op传到下层start
	op := Op{OpType: HandleReceiveShardsOp, SeqId: args.SeqId, ClientId: int64(args.Gid), ConfigOp: ConfigOp{Cfg: args.Cfg, Data: args.Data, TranferringShards: args.TranferringShards}}
	//fmt.Printf("[ ----Server[%v]----] : send a Get,op is :%+v \n", kv.me, op)
	lastIndex, _, _ := kv.rf.Start(op)
	op.Index = lastIndex

	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		//fmt.Printf("[ ----Server[%v]----] : receive a GetAsk :%+v,replyOp:+%v\n", kv.me, args, replyOp)
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.Killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// 确保自己掌管了这个分片并且这个分片没有处在正在迁移的状态中
	shardId := key2shard(args.Key)
	_, exist := kv.TranferringShards[shardId]
	if kv.latestConfig.Shards[shardId] != kv.gid || exist {
		reply.Err = ErrWrongGroup
		return
	}
	// 判断这个分片是否正在迁移中

	// 封装Op传到下层start
	op := Op{OpType: GetOp, Key: args.Key, SeqId: args.SeqId, ClientId: args.ClientId}
	//fmt.Printf("[ ----Server[%v]----] : send a Get,op is :%+v \n", kv.me, op)
	lastIndex, _, _ := kv.rf.Start(op)
	op.Index = lastIndex

	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		//fmt.Printf("[ ----Server[%v]----] : receive a GetAsk :%+v,replyOp:+%v\n", kv.me, args, replyOp)
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvPersist[args.Key]
			kv.mu.Unlock()
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	if kv.Killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// 确保自己掌管了这个分片并且这个分片没有处在正在迁移的状态中
	shardId := key2shard(args.Key)
	_, exist := kv.TranferringShards[shardId]
	if kv.latestConfig.Shards[shardId] != kv.gid || exist {
		reply.Err = ErrWrongGroup
		return
	}
	// 封装Op传到下层start
	op := Op{OpType: args.Op, Key: args.Key, Value: args.Value, SeqId: args.SeqId, ClientId: args.ClientId}
	//fmt.Printf("[ ----Server[%v]----] : is sending a %v,op is :%+v \n", kv.me, args.Op, op)
	lastIndex, _, _ := kv.rf.Start(op)
	op.Index = lastIndex

	DPrintf(11111, "lastIndex为%d的命令:%v", lastIndex, op)

	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case replyOp := <-ch:
		//fmt.Printf("[ ----Server[%v]----] : receive a %vAsk :%+v,Op:%+v\n", kv.me, args.Op, args, replyOp)
		// 通过clientId、seqId确定唯一操作序列
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
			DPrintf(11111, "")

		} else {
			reply.Err = OK
		}

	case <-timer.C:
		reply.Err = ErrWrongLeader
		DPrintf(11111, "已超时")

	}
	defer timer.Stop()
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)

}

func (kv *ShardKV) Killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//func (kv *ShardKV) printOp(op Op) string {
//	return fmt.Sprintf("[op.Index:%v, op.ClientId:%v,op.SeqId:%v,op.Num:%v,op.Shard:%v,op.GID:%v,op.GIDs:%v,op.Servers:%v,op.OpType:%v]，op.Cfg:%v,: ",
//		op.Index, op.ClientId, op.SeqId, op.Num, op.Shard, op.GID, op.GIDs, op.Servers, op.OpType, op.Cfg)
//}

func (kv *ShardKV) sayBasicInfo() string {
	return fmt.Sprintf("[gid:%d, kv.me:%d, kv.latestConfig:%v, kv.SeqId：%v, kv.maxraftstate:%d, kv.Counter:%d] \n", kv.gid, kv.me, kv.latestConfig, kv.SeqId, kv.maxraftstate, kv.Counter)
}

// 每隔500ms轮询一次配置中心以拿到最新的配置
func (kv *ShardKV) detectConfig() {

	for !kv.Killed() {
		// leader负责探测
		if _, isLeader := kv.rf.GetState(); isLeader {
			cfg := kv.mck.Query(-1)
			kv.mu.Lock()
			//kv.oldConfig = kv.latestConfig // 新旧配置转换
			//kv.latestConfig = cfg
			if cfg.Num != kv.latestConfig.Num {
				DPrintf(111, "%s: 探测到配置已经变更为%v", kv.sayBasicInfo(), cfg)
				// 更新一波不一样的配置数，以方便后面计算更新配置的时机
				for shard, gid := range cfg.Shards {
					if gid != kv.latestConfig.Shards[shard] {
						kv.TotalDiffShards++
					}
				}
				configOp := ConfigOp{Cfg: cfg}
				op := Op{ConfigOp: configOp}
				kv.moveData(op)
				kv.mu.Unlock()
				//kv.rf.Start(op)

			}
		}

		// 每隔100ms轮询一次配置中心
		time.Sleep(time.Millisecond * 500)
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.seqMap = make(map[int64]int)
	kv.waitChMap = make(map[int]chan Op)
	kv.dead = 0
	//kv.IsResharding = false
	kv.TranferringShards = make(map[int]struct{})
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.Counter = 0
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	// 一开始就加载配置中心的配置
	//kv.oldConfig = kv.mck.Query(-1)
	kv.latestConfig = kv.mck.Query(-1)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyMsgHandlerLoop()

	go kv.detectConfig()
	return kv
}
