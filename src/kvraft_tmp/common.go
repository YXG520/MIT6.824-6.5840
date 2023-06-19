package kvraft

type Err string
type OpType string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrKilled      Err = "ErrKilled"
	ErrDuplicate   Err = "ErrDuplicate"
	ErrTimeout     Err = "ErrTimeout"
)

const (
	PutOp    OpType = "PutOp"
	AppendOp OpType = "AppendOp"
	GetOp    OpType = "GetOp"
)

// Put or Append
type PutAppendArgs struct {
	Key    string
	Value  string
	OpType OpType // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId    int64
	SequenceNum int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId    int64
	SequenceNum int64
}

type GetReply struct {
	Err   Err
	Value string
}
