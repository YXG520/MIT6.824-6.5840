package shardctrler

//
// Shardctrler clerk.
//

import (
	"MIT6.824-6.5840/labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	seqId    int
	clientId int64 // 标识客户端的唯一ID，可以用于跟踪和关联请求。

	mu sync.Mutex
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
	// Your code here.

	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.seqId++
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.clientId
	args.SeqId = ck.seqId
	//ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// The Join RPC is used by an administrator to add new replica groups.
// Its argument is a set of mappings from unique, non-zero replica group identifiers (GIDs) to lists of server names.
// The shardctrler should react by creating a new configuration that includes the new replica groups.
// The new configuration should divide the shards as evenly as possible among the full set of groups, and should move as
// few shards as possible to achieve that goal. The shardctrler should allow re-use of a GID if it's not part of the current
// configuration (i.e. a GID should be allowed to Join, then Leave, then Join again).
func (ck *Clerk) Join(servers map[int][]string) {
	//ck.mu.Lock()
	//defer ck.mu.Unlock()

	ck.seqId++
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	args.SeqId = ck.seqId
	DPrintf(111, "join: clientId:%d, seqId：%d", ck.clientId, ck.seqId)
	//ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				//fmt.Printf("向%d发送请求,结果返回成功", srv)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {

	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.seqId++
	args.ClientId = ck.clientId
	args.SeqId = ck.seqId
	DPrintf(111, "leave：clientId:%d, seqId：%d", ck.clientId, ck.seqId)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	DPrintf(111, "tester传递的shard是%d, gid是%d", shard, gid)
	args.Shard = shard
	args.GID = gid
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.seqId++
	args.ClientId = ck.clientId
	args.SeqId = ck.seqId

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
