package shardctrler

import (
	"MIT6.824-6.5840/raft"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "MIT6.824-6.5840/labrpc"
import "sync"
import "MIT6.824-6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead      int32
	configs   []Config // indexed by config num
	waitChMap map[int]chan Op
	seqMap    map[int64]int

	//Shards [NShards]int // 分片

}

type Op struct {
	// Your data here.
	ClientId int64
	SeqId    int
	Index    int
	OpType   string
	// joinOp
	Servers map[int][]string // new GID -> servers mappings
	// leaveOp
	GIDs []int
	// moveOp
	Shard int
	GID   int
	// queryOp
	Num int // desired config number
	Cfg Config

	Err Err // 操作op发生的错误
}

func (sc *ShardCtrler) ifDuplicate(clientId int64, seqId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	lastSeqId, exist := sc.seqMap[clientId]
	//DPrintf(111, "check if duplicate: lastSeqId is %d and seqId is %d", lastSeqId, seqId)
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (sc *ShardCtrler) applyMsgHandlerLoop() {
	for {
		if sc.Killed() {
			return
		}
		select {
		case msg := <-sc.applyCh:
			// 如果是命令消息，则应用命令同时响应客户端
			if msg.CommandValid {
				index := msg.CommandIndex
				// 传来的信息快照已经存储了则直接返回

				op := msg.Command.(Op)

				//fmt.Printf("[ ~~~~applyMsgHandlerLoop~~~~ ]: %+v\n", msg)
				if !sc.ifDuplicate(op.ClientId, op.SeqId) {
					sc.mu.Lock()
					switch op.OpType {
					case JoinOp:
						//DPrintf(1111, "节点%d执行join操作", sc.me)
						sc.execJoinCmd(&op) // 注意一定要传地址，而不是值
						//DPrintf(1111, "put后，结果为%v", sc.kvPersist[op.Key])
					case MoveOp:
						sc.execMoveCmd(&op)
						//DPrintf(1111, "Append后，结果为%v", sc.kvPersist[op.Key])
					case QueryOp:
						sc.execQueryCmd(&op)
					case LeaveOp:
						sc.execLeaveCmd(&op)

					}
					sc.seqMap[op.ClientId] = op.SeqId
					// 如果需要日志的容量达到规定值则需要制作快照并且投递
					op.Err = OK
					sc.mu.Unlock()
				}
				// 将返回的ch返回waitCh

				if _, isLead := sc.rf.GetState(); isLead {
					DPrintf(111, "我是节点%d, 通知leader接收信息: %v", sc.me, sc.printOp(op))
					sc.getWaitCh(index) <- op
				}
				//sc.getWaitCh(index) <- op
			} else {
				// lab4a的instruction里没有要求实现快照这一步
				log.Fatalf("执行快照操作")
			}

		}
	}
}

func (sc *ShardCtrler) getWaitCh(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.waitChMap[index]
	if !exist {
		sc.waitChMap[index] = make(chan Op, 1)
		ch = sc.waitChMap[index]
	}
	return ch
}

func (sc *ShardCtrler) printOp(op Op) string {
	return fmt.Sprintf("[op.Index:%v, op.ClientId:%v,op.SeqId:%v,op.Num:%v,op.Shard:%v,op.GID:%v,op.GIDs:%v,op.Servers:%v,op.OpType:%v]，op.Cfg:%v,: ",
		op.Index, op.ClientId, op.SeqId, op.Num, op.Shard, op.GID, op.GIDs, op.Servers, op.OpType, op.Cfg)
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	// 构建调用参数
	reply.WrongLeader = true

	if sc.Killed() {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		//fmt.Printf("%d:不是leader, 打印reply.leader: %v", sc.me, reply.WrongLeader)
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	//fmt.Printf("是leader，准备将命令传递给下层raft")

	op := Op{ClientId: args.ClientId, SeqId: args.SeqId, OpType: JoinOp, Servers: args.Servers}
	// 封装Op传到下层start
	//fmt.Printf("[ ----Server[%v]----] : send a Get,op is :%+v \n", sc.me, op)
	lastIndex, _, _ := sc.rf.Start(op)
	op.Index = lastIndex
	DPrintf(1111, "server接收到join请求:%v", sc.printOp(op))

	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, op.Index)
		sc.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		//fmt.Printf("[ ----Server[%v]----] : receive a GetAsk :%+v,replyOp:+%v\n", sc.me, args, replyOp)
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			reply.WrongLeader = false
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true

	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if sc.Killed() {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true

		return
	}

	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true

		return
	}
	op := Op{ClientId: args.ClientId, SeqId: args.SeqId, OpType: LeaveOp, GIDs: args.GIDs}
	// 封装Op传到下层start
	//fmt.Printf("[ ----Server[%v]----] : send a Get,op is :%+v \n", sc.me, op)
	lastIndex, _, _ := sc.rf.Start(op)
	op.Index = lastIndex

	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, op.Index)
		sc.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		//fmt.Printf("[ ----Server[%v]----] : receive a GetAsk :%+v,replyOp:+%v\n", sc.me, args, replyOp)
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			reply.WrongLeader = false

			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true

	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if sc.Killed() {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true

		return
	}

	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true

		return
	}
	op := Op{ClientId: args.ClientId, SeqId: args.SeqId, OpType: MoveOp, GID: args.GID, Shard: args.Shard}
	// 封装Op传到下层start
	//fmt.Printf("[ ----Server[%v]----] : send a Get,op is :%+v \n", sc.me, op)
	lastIndex, _, _ := sc.rf.Start(op)
	op.Index = lastIndex

	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, op.Index)
		sc.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		//fmt.Printf("[ ----Server[%v]----] : receive a GetAsk :%+v,replyOp:+%v\n", sc.me, args, replyOp)
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			reply.WrongLeader = false

			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true

	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if sc.Killed() {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true

		return
	}
	op := Op{ClientId: args.ClientId, SeqId: args.SeqId, OpType: QueryOp, Num: args.Num}
	// 封装Op传到下层start
	//fmt.Printf("[ ----Server[%v]----] : send a Get,op is :%+v \n", sc.me, op)
	lastIndex, _, _ := sc.rf.Start(op)
	op.Index = lastIndex

	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, op.Index)
		sc.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		//fmt.Printf("[ ----Server[%v]----] : receive a GetAsk :%+v,replyOp:+%v\n", sc.me, args, replyOp)
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			reply.Config = replyOp.Cfg
			DPrintf(111, "我是节点%d, 现在打印返回配置: %v", sc.me, sc.printOp(replyOp))
			reply.WrongLeader = false
			return
		}
	case <-timer.C:
		//_, isLead := sc.rf.GetState()
		//DPrintf(111, "节点%d是否是leader:%v, 但是查询已经超时", sc.me, isLead)
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true

	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)

}
func (sc *ShardCtrler) Killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv_old tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.seqMap = make(map[int64]int)
	sc.waitChMap = make(map[int]chan Op)

	go sc.applyMsgHandlerLoop()
	return sc
}
