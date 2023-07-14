package shardkv

// 更新最新的config的handler
func (kv *ShardKV) updateConfig(op Op) {
	curConfig := kv.Config
	upConfig := op.ConfigOp.Config
	if curConfig.Num >= upConfig.Num {
		DPrintf(111, "拒绝旧配置")
		return
	}
	for shard, gid := range upConfig.Shards {
		if gid == kv.gid && curConfig.Shards[shard] == 0 {
			// 如果更新的配置的gid与当前的配置的gid一样且分片为0(未分配）
			kv.shardPersist[shard].KvMap = make(map[string]string)
			kv.shardPersist[shard].ConfigNum = upConfig.Num
		}
	}
	DPrintf(111, "%v：更新配置前", kv.sayBasicInfo())
	kv.latestConfig = curConfig
	kv.Config = upConfig
	kv.TranferringShards = false
	DPrintf(111, "%v:更新配置成功", kv.sayBasicInfo())
}

// AddShards move shards from caller to this server
func (kv *ShardKV) AddShard(args *MoveShardsArgs, reply *MoveShardsReply) {
	//DPrintf(111, "%v:收到来自复制组%d的分片%d, 分片数据为%v", kv.sayBasicInfo(), args.ClientId, args.ShardId, args.SentShard)
	command := Op{
		OpType:   AddShardHandOp,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		ShardId:  args.ShardId,
		ConfigOp: ConfigOp{SentShard: args.SentShard},
		//Shard:    args.Shard,
		//SeqMap:   args.LastAppliedRequestId,
	}
	reply.Err = kv.startCommand(command, AddShardsTimeout)
	DPrintf(111, "%v:对请求试图发送切片(ClientId:%d, ShardId:%d)的请求的响应状态：%v, ", kv.sayBasicInfo(), args.ClientId, args.ShardId, reply.Err)
	return
}

func (kv *ShardKV) addShardHandler(op Op) {
	// this shard is added or it is an outdated command
	if kv.shardPersist[op.ShardId].KvMap != nil || op.ConfigOp.SentShard.ConfigNum < kv.Config.Num {
		op.Err = ErrStaleConfig
		DPrintf(111, "%v:op.ShardId代表的map对象是否为空：%v, shardId的配置号是否大于传过来的配置号：%v", kv.sayBasicInfo(), (kv.shardPersist[op.ShardId].KvMap != nil), op.ConfigOp.SentShard.ConfigNum < kv.Config.Num)
		return
	}

	kv.shardPersist[op.ShardId] = kv.cloneShard(op.ConfigOp.SentShard.ConfigNum, op.ConfigOp.SentShard.KvMap)
	DPrintf(111, "%v: 成功接收到分片%d", kv.sayBasicInfo(), op.ShardId)
	op.Err = OK
	//for clientId, seqId := range op.SeqMap {
	//	if r, ok := kv.SeqMap[clientId]; !ok || r < seqId {
	//		kv.SeqMap[clientId] = seqId
	//	}
	//}
}

func (kv *ShardKV) removeShardHandler(op Op) {
	if op.SeqId < kv.Config.Num {
		DPrintf(111, "配置还未拉取")
		return
	}
	kv.shardPersist[op.ShardId].KvMap = nil
	kv.shardPersist[op.ShardId].ConfigNum = op.SeqId
	DPrintf(111, "%v: 成功移除分片%d", kv.sayBasicInfo(), op.ShardId)
}
