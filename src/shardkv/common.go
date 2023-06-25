package shardkv

import "MIT6.824-6.5840/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrDuplicate   = "ErrDuplicate"
	ErrStaleConfig = "ErrStaleConfig"
)

const (
	PutOp                 string = "PutOp"
	AppendOp                     = "AppendOp"
	GetOp                        = "GetOp"
	UpdateConfigOp               = "UpdateConfigOp"
	SendShardsOp                 = "SendShardsOp"
	HandleReceiveShardsOp        = "HandleReceiveShardsOp"
)
const (
	JoinOp  string = "JoinOp"
	QueryOp string = "QueryOp"
	LeaveOp string = "LeaveOp"
	MoveOp  string = "MoveOp"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqId    int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	SeqId    int
}

type GetReply struct {
	Err   Err
	Value string
}

type MoveShardsArgs struct {
	Gid               int
	SeqId             int
	Data              map[string]string
	Cfg               shardctrler.Config
	TranferringShards map[int]struct{} // 正在迁移的分片

}
type MoveShardsReply struct {
	Err Err
}
