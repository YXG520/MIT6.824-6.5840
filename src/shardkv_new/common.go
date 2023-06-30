package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                  Err = "OK"
	ErrNoKey                = "ErrNoKey"
	ErrWrongGroup           = "ErrWrongGroup"
	ErrWrongLeader          = "ErrWrongLeader"
	ShardNotArrived         = "ShardNotArrived"
	ConfigNotArrived        = "ConfigNotArrived"
	ErrInconsistentData     = "ErrInconsistentData"
	ErrOverTime             = "ErrOverTime"
)

const (
	PutType         Operation = "Put"
	AppendType                = "Append"
	GetType                   = "Get"
	UpConfigType              = "UpConfig"
	AddShardType              = "AddShard"
	RemoveShardType           = "RemoveShard"
)

type Operation string

type Err string

// PutType or AppendType
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Op        Operation // "Put" or "Append"
	ClientId  int64
	RequestId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientId  int64
	RequestId int
}

type GetReply struct {
	Err   Err
	Value string
}

type SendShardArg struct {
	LastAppliedRequestId map[int64]int // for receiver to update its state
	ShardId              int
	Shard                Shard // Shard to be sent
	ClientId             int64
	RequestId            int
}

type AddShardReply struct {
	Err Err
}
