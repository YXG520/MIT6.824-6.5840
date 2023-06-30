package shardkv

import (
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongGroup    = "ErrWrongGroup"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrDuplicate     = "ErrDuplicate"
	ErrStaleConfig   = "ErrStaleConfig"
	ErrTimeout       = "ErrTimeout"
	ConfigNotArrived = "ConfigNotArrived"
)

const (
	PutOp                       string = "PutOp"
	AppendOp                           = "AppendOp"
	GetOp                              = "GetOp"
	UpdateConfigOp                     = "UpdateConfigOp"
	HandleReceiveShardsOp              = "HandleReceiveShardsOp"
	ErrRefusingSinceConfiguring        = "ErrRefusingSinceConfiguring"
	RemoveShardType                    = "RemoveShardType"
	ErrInconsistentData                = "ErrInconsistentData"
	AddShardHandOp                     = "AddShardHandOp"
)
const (
	UpConfigLoopInterval = 100 * time.Millisecond // poll configuration period

	GetTimeout          = 500 * time.Millisecond
	AppOrPutTimeout     = 500 * time.Millisecond
	UpConfigTimeout     = 500 * time.Millisecond
	AddShardsTimeout    = 500 * time.Millisecond
	RemoveShardsTimeout = 1000 * time.Millisecond
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
	ClientId  int64
	SeqId     int
	SentShard Shard //被发送的分片
	ShardId   int
	SeqMap    map[int64]int
}
type MoveShardsReply struct {
	Err Err
}
