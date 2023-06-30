package shardkv

import (
	"MIT6.824-6.5840/labrpc"
	"MIT6.824-6.5840/shardctrler"
	"fmt"
	"log"
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
	Config       shardctrler.Config
	//oldConfig    shardctrler.Config // 上一次获得的配置

	shardPersist []Shard
	//kvShardMap map[int]map[string]struct{} // 存储分片到数据库关联键的映射，shardId->key map
	SeqId            int
	lastIncludeIndex int             // 最近一次快照的截止的日志索引
	persister        *raft.Persister // 共享raft的持久化地址，方便查找

	Counter int // 分片计数器：计算从获取到最新的配置开始已经收集了多少个分片

	//IsResharding      bool // 是否正在重新分片中
	TranferringShards        bool
	TotalDiffShards          int
	FinishTransferringShards []bool // 所有已经完成接收或者迁移的分片
}
type Shard struct {
	ConfigNum int
	KvMap     map[string]string
}

// 更新配置的操作
type ConfigOp struct {
	SentShard Shard
	Config    shardctrler.Config
}
type Op struct {
	// Your data here.
	ClientId int64
	SeqId    int
	Key      string
	Value    string
	Index    int
	OpType   string
	ShardId  int
	ConfigOp ConfigOp

	Err Err // 操作op发生的错误
}

func (kv *ShardKV) applyMsgHandlerLoop() {
	for {
		if kv.Killed() {
			return
		}
		//DPrintf(111, "applyMsgHandlerLoop running")
		select {
		case msg := <-kv.applyCh:
			// 如果是命令消息，则应用命令同时响应客户端
			if msg.CommandValid {
				index := msg.CommandIndex
				// 传来的信息快照已经存储了则直接返回

				op := msg.Command.(Op)
				if op.OpType == PutOp || op.OpType == GetOp || op.OpType == AppendOp {
					if !kv.ifDuplicate(op.ClientId, op.SeqId) {
						kv.mu.Lock()
						shardId := key2shard(op.Key)
						switch op.OpType {
						case GetOp:
							op.Value = kv.shardPersist[shardId].KvMap[op.Key]
						case PutOp:
							if kv.shardPersist[shardId].KvMap == nil {
								kv.shardPersist[shardId].KvMap = make(map[string]string)
								kv.shardPersist[shardId].ConfigNum = kv.latestConfig.Num
							}
							kv.shardPersist[shardId].KvMap[op.Key] = op.Value
							DPrintf(1111, "%v: put后，结果为%v", kv.sayBasicInfo(), kv.shardPersist[shardId].KvMap[op.Key])
						case AppendOp:
							DPrintf(1111, "%v: append前值为%v, append后,值为%v", kv.sayBasicInfo(), kv.shardPersist[shardId].KvMap[op.Key], kv.shardPersist[shardId].KvMap[op.Key]+op.Value)
							kv.shardPersist[shardId].KvMap[op.Key] += op.Value
						default:
							log.Fatalf("invalid command type: %v.", op.OpType)
						}

						kv.mu.Unlock()
					}
				} else {
					kv.mu.Lock()
					switch op.OpType {
					case AddShardHandOp:
						DPrintf(111, "%v: 准备接收分片%d", kv.sayBasicInfo(), op.ShardId)
						if kv.Config.Num < op.SeqId {
							op.Err = ConfigNotArrived
							DPrintf(111, "%v: 配置未到达...", kv.sayBasicInfo())
							break
						}
						kv.addShardHandler(op)

					case UpdateConfigOp:
						kv.updateConfig(op)

					case RemoveShardType:
						DPrintf(111, "%v:开始执行移除操作,op.ShardId：%d, op.ClientId：%d, op.SeqId：%d", kv.sayBasicInfo(), op.ShardId, op.ClientId, op.SeqId)
						kv.removeShardHandler(op)
					default:
						log.Fatalf("invalid command type: %v.", op.OpType)
					}
					kv.mu.Unlock()

				}

				if op.ClientId != -1 {
					kv.mu.Lock()
					kv.seqMap[op.ClientId] = op.SeqId
					kv.mu.Unlock()
					//DPrintf(111, "设置seqMap防重复: %d", kv.seqMap[op.ClientId])
				}
				// 如果需要日志的容量达到规定值则需要制作快照并且投递
				if kv.isNeedSnapshot() {
					go kv.makeSnapshot(msg.CommandIndex)
					//go kv.deliverSnapshot()
				}
				// 将返回的ch返回waitCh
				if _, isLead := kv.rf.GetState(); isLead {
					//DPrintf(111, "%v: 是lead，返回关于%v操作", kv.sayBasicInfo(), op.OpType)
					DPrintf(111, "%v:是leader，返回channel之前，打印数据状态:op.Err:%v, op.OpType：%v", kv.sayBasicInfo(), op.Err, op.OpType)
					kv.getWaitCh(index) <- op
				} else {
					//DPrintf(111, "%v: 不是lead，拒绝通知关于%v的操作，但是操作执行成功", kv.sayBasicInfo(), op.OpType)
				}
			} else if msg.SnapshotValid {
				// 如果是raft传递上来的快照消息，就应用快照，但是不需要响应客户
				//DPrintf(11111, "节点%d应用快照", kv.me)
				kv.decodeSnapshot(msg.SnapshotIndex, msg.Snapshot)
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}
func (kv *ShardKV) allSent() bool {
	for shard, gid := range kv.latestConfig.Shards {
		// 如果当前配置中分片中的信息不匹配，且持久化中的配置号更小，说明还未发送
		// gid == kv.gid：识别原来属于自己的shards
		// kv.Config.Shards[shard] != kv.gid：在新配置中这些shards不属于自己才能说明是需要迁移的分片
		// kv.shardPersist[shard].ConfigNum < kv.Config.Num：如果已经移出成功，则kv.shardPersist[shard].ConfigNum==kv.Config.Num,
		if gid == kv.gid && kv.Config.Shards[shard] != kv.gid && kv.shardPersist[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	DPrintf(111, "%v:所有待迁出分片，迁出成功", kv.sayBasicInfo())
	return true
}

func (kv *ShardKV) allReceived() bool {
	for shard, gid := range kv.latestConfig.Shards {
		// gid != kv.gid: 原来的配置中该分片不属于自己
		// kv.Config.Shards[shard] == kv.gid: 新配置中分片属于自己
		// 如果收到了一个分片，则其分片的版本号kv.shardPersist[shard].ConfigNum == kv.Config.Num
		// 判断切片是否都收到了
		if gid != kv.gid && kv.Config.Shards[shard] == kv.gid && kv.shardPersist[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	DPrintf(111, "%v:所有待迁入分片，迁入成功", kv.sayBasicInfo())
	return true
}
func (kv *ShardKV) sendShard(servers []*labrpc.ClientEnd, args *MoveShardsArgs, shardId int) {
	DPrintf(111, "%v: 向复制组%d发送分片%d", kv.sayBasicInfo(), kv.Config.Shards[shardId], shardId)

	index := 0
	start := time.Now()
	for {
		DPrintf(111, "%v: 准备向复制组%d(组内实例：%v)发送分片%d", kv.sayBasicInfo(), kv.Config.Shards[shardId], servers[index], shardId)

		var reply MoveShardsReply
		// 对自己的共识组内进行add
		ok := servers[index].Call("ShardKV.AddShard", args, &reply)

		DPrintf(111, "%v:打印来自复制组%d关于接收切片%d响应结果: %v", kv.sayBasicInfo(), kv.Config.Shards[shardId], shardId, reply.Err)
		// 如果给予切片成功，或者时间超时，这两种情况都需要进行GC掉不属于自己的切片
		if ok && reply.Err == OK || time.Now().Sub(start) >= 2*time.Second {
			DPrintf(111, "%v：成功收到来自复制组%d关于接收切片%d的响应！！准备移除分片", kv.sayBasicInfo(), kv.Config.Shards[shardId], shardId)
			// 如果成功,立即发送移除该分片的指令到raft集群中
			//kv.mu.Lock()
			command := Op{
				OpType:   RemoveShardType,
				ClientId: int64(kv.gid),
				SeqId:    kv.Config.Num,
				ShardId:  args.ShardId,
			}
			//kv.mu.Unlock()
			err := kv.startCommand(command, RemoveShardsTimeout)
			DPrintf(111, "%v:删除分片%d的结果：%v", kv.sayBasicInfo(), args.ShardId, err)
			break
		}
		index = (index + 1) % len(servers)
		if index == 0 {
			time.Sleep(UpConfigLoopInterval)
		}
	}
	DPrintf(111, "%v: 成功向复制组%d(组内实例：%v)发送分片%d", kv.sayBasicInfo(), kv.Config.Shards[shardId], servers[index], shardId)

}

// ConfigDetectedLoop 配置检测
func (kv *ShardKV) detectConfig() {
	kv.mu.Lock()

	curConfig := kv.Config
	rf := kv.rf
	kv.mu.Unlock()

	for !kv.Killed() {
		// only leader needs to deal with configuration tasks
		if _, isLeader := rf.GetState(); !isLeader {
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		kv.mu.Lock()
		// 判断是否把不属于自己的部分给分给别人了
		if !kv.allSent() {
			DPrintf(111, "分片没有全部发送成功")

			kv.TranferringShards = true

			for shardId, gid := range kv.latestConfig.Shards {
				// 将最新配置里不属于自己的分片分给别人
				if gid == kv.gid && kv.Config.Shards[shardId] != kv.gid && kv.shardPersist[shardId].ConfigNum < kv.Config.Num {
					DPrintf(111, "%v: 遍历到第%d个分片", kv.sayBasicInfo(), shardId)
					sendData := kv.cloneShard(kv.Config.Num, kv.shardPersist[shardId].KvMap)

					args := MoveShardsArgs{
						ShardId:   shardId,
						SentShard: sendData,
						ClientId:  int64(gid),
						SeqId:     kv.Config.Num,
					}

					// shardId -> gid -> server names
					serversList := kv.Config.Groups[kv.Config.Shards[shardId]]
					servers := make([]*labrpc.ClientEnd, len(serversList))
					for i, name := range serversList {
						servers[i] = kv.make_end(name)
					}
					// 开启协程对每个客户端发送切片(这里发送的应是别的组别，自身的共识组需要raft进行状态修改）
					kv.sendShard(servers, &args, shardId)
				}
			}
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		if !kv.allReceived() {
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			DPrintf(111, "%v:分片没有全部接收成功", kv.sayBasicInfo())
			continue
		}
		DPrintf(111, "%v:分片全部迁移完成，准备拉取新配置", kv.sayBasicInfo())
		// current configuration is configured, poll for the next configuration
		curConfig = kv.Config
		sck := kv.mck
		kv.mu.Unlock()
		time.Sleep(UpConfigLoopInterval)

		newConfig := sck.Query(curConfig.Num + 1)
		//DPrintf(111, "%v:查询到的配置为%v", kv.sayBasicInfo(), newConfig)

		if newConfig.Num != curConfig.Num+1 {
			time.Sleep(UpConfigLoopInterval)
			DPrintf(111, "%v:查询到的配置不是连续的%v", kv.sayBasicInfo(), newConfig)
			continue
		} else {
			DPrintf(111, "%v:查询到的配置为%v", kv.sayBasicInfo(), newConfig)
		}

		command := Op{
			OpType:   UpdateConfigOp,
			ClientId: int64(kv.gid),
			SeqId:    newConfig.Num,
			ConfigOp: ConfigOp{Config: newConfig},
		}
		kv.startCommand(command, UpConfigTimeout)
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
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan Op, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

func (kv *ShardKV) unregisterTask(index int) {
	//kv.mu.Lock()
	delete(kv.waitChMap, index)
	//kv.mu.Unlock()
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
	kv.mu.Lock()
	// 如果正在迁移分片就拒绝用户的请求
	if kv.TranferringShards {
		DPrintf(111, "检测到该复制组正在迁移数据，在此期间，会拒绝所有请求")
		reply.Err = ErrRefusingSinceConfiguring
		kv.mu.Unlock()
		return
	}

	// 确保自己掌管了这个分片并且这个分片没有处在正在迁移的状态中
	shardId := key2shard(args.Key)
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()

		return
	}
	kv.mu.Unlock()

	// 判断这个分片是否正在迁移中

	// 封装Op传到下层start
	op := Op{OpType: GetOp, Key: args.Key, SeqId: args.SeqId, ClientId: args.ClientId}
	//fmt.Printf("[ ----Server[%v]----] : send a Get,op is :%+v \n", kv.me, op)
	lastIndex, _, _ := kv.rf.Start(op)
	op.Index = lastIndex

	ch := kv.getWaitCh(lastIndex)
	//defer func() {
	//	kv.mu.Lock()
	//	delete(kv.waitChMap, op.Index)
	//	kv.mu.Unlock()
	//}()

	defer kv.unregisterTask(op.Index)

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
			reply.Value = replyOp.Value
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
	DPrintf(111, "%v:收到了一个Put请求:%v, 其shardId为%d", kv.sayBasicInfo(), args.Op, key2shard(args.Key))
	kv.mu.Lock()
	// 如果正在迁移分片就拒绝所有用户的请求
	if kv.TranferringShards {
		DPrintf(111, "%v:检测到该复制组正在迁移数据，在此期间，会拒绝所有请求", kv.sayBasicInfo())
		reply.Err = ErrRefusingSinceConfiguring
		kv.mu.Unlock()
		return
	} else {
		DPrintf(11, "%v: 通过该请求", kv.sayBasicInfo())
	}
	// 确保自己掌管了这个分片
	shardId := key2shard(args.Key)
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	// 封装Op传到下层start
	op := Op{OpType: args.Op, Key: args.Key, Value: args.Value, SeqId: args.SeqId, ClientId: args.ClientId}
	//fmt.Printf("[ ----Server[%v]----] : is sending a %v,op is :%+v \n", kv.me, args.Op, op)
	lastIndex, _, _ := kv.rf.Start(op)
	op.Index = lastIndex

	DPrintf(11111, "lastIndex为%d的命令:%v", lastIndex, op)

	ch := kv.getWaitCh(lastIndex)
	//defer func() {
	//	kv.mu.Lock()
	//	delete(kv.waitChMap, op.Index)
	//	kv.mu.Unlock()
	//}()

	defer kv.unregisterTask(op.Index)

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
	return fmt.Sprintf("[gid:%d, kv.me:%d, kv.latestConfig：%v，\n kv.curConfig:%v, kv.SeqId：%v, kv.maxraftstate:%d, "+
		"kv.Counter:%d] \n， [kv.shardPersist:%v]", kv.gid, kv.me, kv.latestConfig, kv.Config, kv.SeqId, kv.maxraftstate, kv.Counter, kv.shardPersist)
}

func (kv *ShardKV) startCommand(command Op, timeoutPeriod time.Duration) Err {
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		//command.Err = ErrWrongLeader
		return ErrWrongLeader
	}
	command.Index = index
	DPrintf(111, "%v: 操作类型是%v", kv.sayBasicInfo(), command.OpType)
	ch := kv.getWaitCh(index)
	defer kv.unregisterTask(command.Index)

	timer := time.NewTicker(timeoutPeriod)
	defer timer.Stop()

	select {
	case re := <-ch:
		//DPrintf(111, "收到了")
		//kv.mu.Lock()
		if re.SeqId != command.SeqId || re.ClientId != command.ClientId {
			// One way to do this is for the server to detect that it has lost leadership,
			// by noticing that a different request has appeared at the index returned by Start()
			//kv.mu.Unlock()
			//command.Err = ErrInconsistentData
			return ErrInconsistentData
		}
		//kv.mu.Unlock()
		//re.Err = OK
		//command.Err = re.Err
		DPrintf(111, "%v:成功执行了%v操作", kv.sayBasicInfo(), command.OpType)
		return OK
	case <-timer.C:
		//command.Err = ErrTimeout
		DPrintf(111, "%v:%v操作超时", kv.sayBasicInfo(), command.OpType)
		return ErrTimeout
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
	kv.persister = persister
	kv.seqMap = make(map[int64]int)
	kv.waitChMap = make(map[int]chan Op)
	kv.shardPersist = make([]Shard, shardctrler.NShards)

	kv.dead = 0
	//kv.IsResharding = false
	kv.TranferringShards = true
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.Counter = 0
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.decodeSnapshot(kv.rf.GetLastIncludeIndex(), kv.persister.ReadSnapshot())
	//kv.decodeSnapshot()
	go kv.applyMsgHandlerLoop()

	go kv.detectConfig()
	return kv
}
