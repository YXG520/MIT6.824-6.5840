package shardkv

import (
	"time"
)

// 获取配置后需要迁移数据
func (kv *ShardKV) moveData(op Op) {

	// 获取最新的配置信息
	shards := op.ConfigOp.Cfg.Shards
	targetReplicaGroups := op.ConfigOp.Cfg.Groups
	// 首先需要标记所有正在迁移的分片
	//for _, shardId := range shards {
	//	kv.TranferringShards[shardId] = struct{}{}
	//}
	// 创建一个map，其中key是分片ID，value是属于该分片的key集合
	kvShardMap := make(map[int]map[string]struct{})
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	// 遍历存储的键值对
	for key, _ := range kv.kvPersist {
		// 使用key2shard函数确定此键属于哪个分片
		shard := key2shard(key)

		// 将键添加到相应的分片集合中
		if _, ok := kvShardMap[shard]; !ok {
			kvShardMap[shard] = make(map[string]struct{})
		}
		kvShardMap[shard][key] = struct{}{}
	}
	// 记录需要往哪些复制组里迁移哪些数据分片
	gid2keys := make(map[int][]string)
	// 根据最新的配置，确定哪些分片需要迁移
	for shard, keys := range kvShardMap {
		// 获取这个分片应该属于的复制组ID
		targetReplicaGroupId := shards[shard]

		// 检查此分片是否应该迁移到新的复制组
		// 如果是则需要加入到迁入map中
		// 如果在旧配置中一个分片属于自己管但是新配置中不是自己管理的分片就需要迁移出去
		if kv.latestConfig.Shards[shard] == kv.gid && targetReplicaGroupId != kv.gid {
			kv.TranferringShards[shard] = struct{}{}
			for key := range keys {
				gid2keys[targetReplicaGroupId] = append(gid2keys[targetReplicaGroupId], key)
			}
		}
	}

	// 准备迁移...
	//if len(gid2keys) == 0 {
	//	DPrintf(111, "无需迁移,但是")
	//	// 即使无需迁移，下发状态到的raft系统通知其他的从节点更新自己的数据库
	//	op.OpType = UpdateConfigOp
	//	kv.rf.Start(op)
	//	return
	//}
	op.ConfigOp.TranferringShards = kv.TranferringShards
	kv.moveAll2targetServers(op, gid2keys, targetReplicaGroups)

	// 迁移成功后，下发状态到的raft系统通知其他的从节点更新自己的数据库
	op.OpType = UpdateConfigOp
	kv.rf.Start(op)

}

// 将所有发生变更的分片发送到目标复制组
func (kv *ShardKV) moveAll2targetServers(op Op, gid2keys map[int][]string, targetReplicaGroups map[int][]string) {

	for gid, keys := range gid2keys {
		// 获取目标复制组
		targetReplicaGroupNames := targetReplicaGroups[gid]
		// 复制键值对
		data := make(map[string]string)
		for _, k := range keys {
			data[k] = kv.kvPersist[k]
		}
		kv.sendShards2TargetGroup(op, data, targetReplicaGroupNames)

	}
}

func (kv *ShardKV) sendShards2TargetGroup(op Op, data map[string]string, targetReplicaGroupNames []string) {

	kv.SeqId++
	args := MoveShardsArgs{Gid: kv.gid, SeqId: kv.SeqId, Data: data, Cfg: op.ConfigOp.Cfg, TranferringShards: op.ConfigOp.TranferringShards}

	for {
		// try each known server.
		for _, serverName := range targetReplicaGroupNames {
			var reply MoveShardsReply
			ok := kv.make_end(serverName).Call("ShardKV.HandleReceiveShardingRPC", &args, &reply)
			// 直到Ok才返回
			if ok && reply.Err == OK {
				//fmt.Printf("向%d发送请求,结果返回成功", srv)
				return
			}
			if ok && reply.Err == ErrStaleConfig {
				//fmt.Printf("向%d发送请求,结果返回成功", srv)
				DPrintf(111, "接收方已经正确更新分片：ErrStaleConfig")
				return
			}

		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 尝试一次数据库和配置的更新
func (kv *ShardKV) updateDBAndConfig(op Op) {
	// 迁移成功后删除所有已经迁出的分片并且更新本地配置
	for _, k := range op.ConfigOp.Data {
		delete(kv.kvPersist, k)
	}
	kv.Counter += len(op.ConfigOp.TranferringShards)
	DPrintf(111, "%v: 此次完成了将%d个分片完成投递，尝试一次更新配置操作", kv.sayBasicInfo(), len(op.ConfigOp.TranferringShards))
	// 尝试一次更新配置操作
	kv.attemptUpdateConfig(op)
}
func (kv *ShardKV) attemptUpdateConfig(op Op) {
	// 如果所有的分片都迁移完毕
	if kv.Counter == kv.TotalDiffShards {
		kv.latestConfig = op.ConfigOp.Cfg
		kv.Counter = 0
		kv.TranferringShards = make(map[int]struct{}) //清空set
		DPrintf(111, "%v: 所有分片都发送或者接收完毕，更新配置成功", kv.sayBasicInfo())
	}
}

// 各个节点的状态机将接收的分片应用到数据库
func (kv *ShardKV) HandleReceiveShardsOp(op Op) {
	data := op.ConfigOp.Data
	//kv.mu.Lock()
	//defer kv.mu.Unlock()

	for k, v := range data {
		kv.kvPersist[k] = v
	}
	kv.Counter += len(op.ConfigOp.TranferringShards)
	kv.attemptUpdateConfig(op)
}
