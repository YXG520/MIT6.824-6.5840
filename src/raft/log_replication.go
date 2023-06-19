package raft

import (
	"time"
)

func (rf *Raft) pastHeartbeatTimeout() bool {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	return time.Since(rf.lastHeartbeat) > rf.heartbeatTimeout
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	rf.lastHeartbeat = time.Now()
}

//
//// 方法一：定义一个心跳兼日志同步处理器，这个方法是Candidate和Follower节点的处理
//// nextIndex递减算法
//func (rf *Raft) HandleAppendEntriesRPC2(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
//	rf.mu.Lock() // 加接收日志方的锁
//	defer rf.mu.Unlock()
//	reply.FollowerTerm = rf.currentTerm
//	reply.Success = true
//	// 旧任期的leader抛弃掉
//	if args.LeaderTerm < rf.currentTerm {
//		reply.Success = false
//		return
//	}
//	rf.resetElectionTimer()
//	rf.state = Follower // 需要转变自己的身份为Follower
//
//	if args.LeaderTerm > rf.currentTerm {
//		rf.votedFor = None // 调整votedFor为-1
//		rf.currentTerm = args.LeaderTerm
//	}
//
//	if args.PrevLogIndex+1 < rf.log.FirstLogIndex || args.PrevLogIndex > rf.log.LastLogIndex || rf.getEntryTerm(args.PrevLogIndex) != args.PrevLogTerm {
//		DPrintf(111, "args.PrevLogIndex is %d, out of index...", args.PrevLogIndex)
//		reply.FollowerTerm = rf.currentTerm
//		reply.Success = false
//		reply.PrevLogIndex = rf.log.LastLogIndex
//		reply.PrevLogTerm = rf.getLastEntryTerm()
//	} else if rf.getEntryTerm(args.PrevLogIndex) == args.PrevLogTerm {
//		ok := true
//		for i, entry := range args.Entries {
//			index := args.PrevLogIndex + 1 + i
//			if index > rf.log.LastLogIndex {
//				rf.log.appendL(entry)
//			} else if rf.log.getOneEntry(index).Term != entry.Term {
//				// 采用覆盖写的方式
//				ok = false
//				*rf.log.getOneEntry(index) = entry
//			}
//		}
//		if !ok {
//			rf.log.LastLogIndex = args.PrevLogIndex + len(args.Entries)
//		}
//		if args.LeaderCommit > rf.commitIndex {
//			if args.LeaderCommit < rf.log.LastLogIndex {
//				rf.commitIndex = args.LeaderCommit
//			} else {
//				rf.commitIndex = rf.log.LastLogIndex
//			}
//			rf.applyCond.Broadcast()
//		}
//		reply.FollowerTerm = rf.currentTerm
//		reply.Success = true
//		reply.PrevLogIndex = rf.log.LastLogIndex
//		reply.PrevLogTerm = rf.getLastEntryTerm()
//		DPrintf(200, "%v:log entries was overrited, added or done nothing, updating commitIndex to %d...", rf.SayMeL(), rf.commitIndex)
//	}
//
//}

// nextIndex收敛速度优化：nextIndex跳跃算法
// 定义一个心跳兼日志同步处理器，这个方法是Candidate和Follower节点的处理
func (rf *Raft) HandleAppendEntriesRPC(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.Mu.Lock() // 加接收日志方的锁
	defer rf.Mu.Unlock()
	reply.FollowerTerm = rf.currentTerm
	reply.Success = true
	// 旧任期的leader抛弃掉
	if args.LeaderTerm < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.resetElectionTimer()
	rf.state = Follower // 需要转变自己的身份为Follower

	if args.LeaderTerm > rf.currentTerm {
		rf.votedFor = None // 调整votedFor为-1
		rf.currentTerm = args.LeaderTerm
		reply.FollowerTerm = rf.currentTerm
	}
	defer rf.persist()

	if rf.log.empty() {
		// 首先可以确定的是，主结点的args.PrevLogIndex = min(rf.nextIndex[i]-1, rf.lastLogIndex)
		// 这可以比从节点的rf.snapshotLastIncludeIndex大、小或者等价， 因为可以根据
		// args.PrevLogIndex的计算式子得出，nextIndex在leader刚选出时是0，
		// 日志为空，要么是节点刚启动的初始状态，要么是被快照截断后的状态
		// 在初始状态，两者都是0，从节点可以全部接收日志，
		// 若被日志截断，则rf.snapshotLastIncludeIndex前面的日志都是无效的，
		// args.PrevLogIndex >  rf.snapshotLastIncludeIndex 这部分
		// 日志肯定不能插入，所以也会丢弃
		if args.PrevLogIndex == rf.snapshotLastIncludeIndex {
			rf.log.appendL(args.Entries...)
			reply.FollowerTerm = rf.currentTerm
			reply.Success = true
			reply.PrevLogIndex = rf.log.LastLogIndex
			reply.PrevLogTerm = rf.getLastEntryTerm()
			return
		} else {
			reply.FollowerTerm = rf.currentTerm
			reply.Success = false
			reply.PrevLogIndex = rf.log.LastLogIndex
			reply.PrevLogTerm = rf.getLastEntryTerm()
			return
		}
	}
	if args.PrevLogIndex+1 < rf.log.FirstLogIndex || args.PrevLogIndex > rf.log.LastLogIndex {
		DPrintf(111, "args.PrevLogIndex is %d, out of index...", args.PrevLogIndex)
		reply.FollowerTerm = rf.currentTerm
		reply.Success = false
		reply.PrevLogIndex = rf.log.LastLogIndex
		reply.PrevLogTerm = rf.getLastEntryTerm()
	} else if rf.getEntryTerm(args.PrevLogIndex) == args.PrevLogTerm {
		ok := true
		for i, entry := range args.Entries {
			index := args.PrevLogIndex + 1 + i
			if index > rf.log.LastLogIndex {
				rf.log.appendL(entry)
			} else if rf.log.getOneEntry(index).Term != entry.Term {
				// 采用覆盖写的方式
				ok = false
				*rf.log.getOneEntry(index) = entry
			}
		}
		if !ok {
			rf.log.LastLogIndex = args.PrevLogIndex + len(args.Entries)
		}
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < rf.log.LastLogIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.log.LastLogIndex
			}
			rf.applyCond.Broadcast()
		}
		reply.FollowerTerm = rf.currentTerm
		reply.Success = true
		reply.PrevLogIndex = rf.log.LastLogIndex
		reply.PrevLogTerm = rf.getLastEntryTerm()
		DPrintf(200, "%v:log entries was overrited, added or done nothing, updating commitIndex to %d...", rf.SayMeL(), rf.commitIndex)
	} else {
		prevIndex := args.PrevLogIndex
		for prevIndex >= rf.log.FirstLogIndex && rf.getEntryTerm(prevIndex) == rf.log.getOneEntry(args.PrevLogIndex).Term {
			prevIndex--
		}
		//prevIndex++ // 当前任期提交的第一个日志
		reply.FollowerTerm = rf.currentTerm
		reply.Success = false
		if prevIndex >= rf.log.FirstLogIndex {
			reply.PrevLogIndex = prevIndex
			reply.PrevLogTerm = rf.getEntryTerm(prevIndex)
			DPrintf(111, "%v: stepping over the index of currentTerm to the last log entry of last term", rf.SayMeL())
		} else {
			// 小于rf.log.FirstLogIndex则需要使用 rf.snapshotLastIncludeIndex
			reply.PrevLogIndex = rf.snapshotLastIncludeIndex
			reply.PrevLogTerm = rf.snapshotLastIncludeTerm
		}

	}

}

// 主节点对日志进行提交，其条件是多余一半的从节点的commitIndex>=leader节点当前提交的commitIndex
func (rf *Raft) tryCommitL(matchIndex int) {
	if matchIndex <= rf.commitIndex {
		// 首先matchIndex应该是大于leader节点的commitIndex才能提交，因为commitIndex及其之前的不需要更新
		return
	}
	// 越界的也不能提交
	if matchIndex > rf.log.LastLogIndex {
		return
	}
	if matchIndex < rf.log.FirstLogIndex {
		return
	}
	// 提交的必须本任期内从客户端收到的日志
	if rf.getEntryTerm(matchIndex) != rf.currentTerm {
		return
	}

	// 计算所有已经正确匹配该matchIndex的从节点的票数
	cnt := 1 //自动计算上leader节点的一票
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// 为什么只需要保证提交的matchIndex必须小于等于其他节点的matchIndex就可以认为这个节点在这个matchIndex记录上正确匹配呢？
		// 因为matchIndex是增量的，如果一个从节点的matchIndex=10，则表示该节点从1到9的子日志都和leader节点对上了
		if matchIndex <= rf.peerTrackers[i].matchIndex {
			cnt++
		}
	}
	//DPrintf(2, "%v: rf.commitIndex = %v ,trycommitindex=%v,matchindex=%v cnt=%v", rf.SayMeL(), rf.commitIndex, index, rf.matchIndex, cnt)
	// 超过半数就提交
	if cnt > len(rf.peers)/2 {
		rf.commitIndex = matchIndex
		if rf.commitIndex > rf.log.LastLogIndex {
			DPrintf(999, "%v: commitIndex > lastlogindex %v > %v", rf.SayMeL(), rf.commitIndex, rf.log.LastLogIndex)
			panic("")
		}
		// DPrintf(500, "%v: commitIndex = %v ,entries=%v", rf.SayMeL(), rf.commitIndex, rf.log.Entries)
		DPrintf(199, "%v: 主结点已经提交了index为%d的日志，rf.applyCond.Broadcast(),rf.lastApplied=%v rf.commitIndex=%v", rf.SayMeL(), rf.commitIndex, rf.lastApplied, rf.commitIndex)
		rf.applyCond.Broadcast() // 通知对应的applier协程将日志放到状态机上验证
	} else {
		DPrintf(199, "\n%v: 未超过半数节点在此索引上的日志相等，拒绝提交....\n", rf.SayMeL())
	}
}
