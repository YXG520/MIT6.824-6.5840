package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"MIT6.824-6.5840/labrpc"
)
import "crypto/rand"
import "math/big"
import "MIT6.824-6.5840/shardctrler"
import "time"

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk // 配置中心的客户端，供我们调用以获取最新的服务
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	seqId    int
	clientId int64 // 标识客户端的唯一ID，可以用于跟踪和关联请求。

	//mu sync.Mutex
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.seqId = 0
	// 初始化时就向配置中心获取配置并且更新到本地缓存中
	ck.config = ck.sm.Query(-1)

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	//ck.mu.Lock()

	ck.seqId++
	args.ClientId = ck.clientId
	args.SeqId = ck.seqId
	//ck.mu.Unlock()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey || reply.Err == ErrDuplicate) {
					DPrintf(111, "从配置组%d中正确的返回了shardId为%d的key对应的value:%v", gid, shard, reply.Value)
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}

				// ... not ok, or ErrWrongLeader
				//if ok && (reply.Err == ErrWrongLeader)
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		//ck.mu.Lock()
		ck.config = ck.sm.Query(-1) // 典型的懒加载或者按需加载
		//ck.mu.Unlock()
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	//ck.mu.Lock()
	ck.seqId++
	args.ClientId = ck.clientId
	args.SeqId = ck.seqId
	//ck.mu.Unlock()

	for {
		shard := key2shard(key)
		//args.shardId = shard
		//ck.mu.Lock()

		gid := ck.config.Shards[shard]
		//ck.mu.Unlock()
		if servers, ok := ck.config.Groups[gid]; ok {
			//ck.mu.Unlock()
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey || reply.Err == ErrDuplicate) {
					//DPrintf(111, "成功PutAppend， reply.Err：%v", reply.Err)
					DPrintf(111, "从复制组%d中正确的put/append(%v)了shardId为%d的key, 其对应的value:%v", gid, args.Op, shard, args.Value)

					return
				} else if ok && reply.Err == ErrWrongGroup {
					break
				} else {
					//DPrintf(111, "err wrong leader")
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(130 * time.Millisecond)
		// ask controler for the latest configuration.
		//ck.mu.Lock()
		ck.config = ck.sm.Query(-1)
		//ck.mu.Unlock()

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendOp)
}
