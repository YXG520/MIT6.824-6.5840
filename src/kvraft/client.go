package kvraft

import (
	"MIT6.824-6.5840/labrpc"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	seqId    int
	leaderId int // 确定哪个服务器是leader，下次直接发送给该服务器
	clientId int64
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	//ck.leaderId = mathrand.Intn(len(ck.servers))
	ck.leaderId = int(nrand()) % len(ck.servers)

	return ck
}
func (ck *Clerk) Get(key string) string {
	//DPrintf(1111, "调用get")
	// You will have to modify this function.
	ck.seqId++
	args := GetArgs{Key: key, ClientId: ck.clientId, SeqId: ck.seqId}
	serverId := ck.leaderId
	for {

		reply := GetReply{}
		//fmt.Printf("[ ++++Client[%v]++++] : send a Get,args:%+v,serverId[%v]\n", ck.clientId, args, serverId)
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == ErrNoKey {
				ck.leaderId = serverId
				return ""
			} else if reply.Err == OK {
				ck.leaderId = serverId
				return reply.Value
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}

		// 节点发生crash等原因
		serverId = (serverId + 1) % len(ck.servers)

	}

}
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId++
	serverId := ck.leaderId

	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, SeqId: ck.seqId}
	for {

		reply := PutAppendReply{}
		//fmt.Printf("[ ++++Client[%v]++++] : send a %v,serverId[%v] : serverId:%+v\n", ck.clientId, op, args, serverId)
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}

		serverId = (serverId + 1) % len(ck.servers)

	}

}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
func (ck *Clerk) Put(key string, value string) {
	//DPrintf(1111, "调用put")
	ck.PutAppend(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {
	//DPrintf(1111, "调用Append")

	ck.PutAppend(key, value, AppendOp)
}
