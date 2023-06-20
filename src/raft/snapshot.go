package raft

import "fmt"

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	if rf.state != Leader {
		return
	}
	DPrintf(111, "%v: come Snapshot index=%v", rf.SayMeL(), index)
	if rf.log.FirstLogIndex <= index {
		if index > rf.lastApplied {
			panic(fmt.Sprintf("%v: index=%v rf.lastApplied=%v\n", rf.SayMeL(), index, rf.lastApplied))
		}
		rf.snapshot = snapshot
		rf.snapshotLastIncludeIndex = index
		rf.snapshotLastIncludeTerm = rf.getEntryTerm(index)
		// Snapshot{
		// 	LastIncludeIndex: index,
		// 	LastIncludeTerm:  rf.getEntryTerm(index),
		// 	Data:             snapshot,
		// }
		newFirstLogIndex := index + 1
		if newFirstLogIndex <= rf.log.LastLogIndex {
			rf.log.Entries = rf.log.Entries[newFirstLogIndex-rf.log.FirstLogIndex:]
			DPrintf(111, "%v: 被快照截断后的日志为: %v", rf.SayMeL(), rf.log.Entries)
		} else {
			rf.log.LastLogIndex = newFirstLogIndex - 1
			rf.log.Entries = make([]Entry, 0)
		}
		rf.log.FirstLogIndex = newFirstLogIndex
		rf.commitIndex = max(rf.commitIndex, index)
		rf.lastApplied = max(rf.lastApplied, index)
		DPrintf(111, "%v:进行快照后，更新commitIndex为%d, lastApplied为%d, "+
			"但是snapshotLastIncludeIndex是%d", rf.SayMeL(), rf.commitIndex, rf.lastApplied, rf.snapshotLastIncludeIndex)

		rf.persist()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.InstallSnapshot(i)
		}
		//DPrintf(11, "%v: len(rf.log.Entries)=%v rf.log.FirstLogIndex=%v rf.log.LastLogIndex=%v rf.commitIndex=%v  rf.lastApplied=%v\n",
		//	rf.SayMeL(), len(rf.log.Entries), rf.log.FirstLogIndex, rf.log.LastLogIndex, rf.commitIndex, rf.lastApplied)
	}

}

func (rf *Raft) RequestInstallSnapshot(args *RequestInstallSnapShotArgs, reply *RequestInstallSnapShotReply) {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	//defer DPrintf(11, "%v: RequestInstallSnapshot end  args.LeaderId=%v, args.LastIncludeIndex=%v, args.LastIncludeTerm=%v\n", rf.SayMeL(), args.LeaderId, args.LastIncludeIndex, args.LastIncludeTerm)
	DPrintf(111, "%v: receiving snapshot from LeaderId=%v with args.LastIncludeIndex=%v, args.LastIncludeTerm=%v\n", rf.SayMeL(), args.LeaderId, args.LastIncludeIndex, args.LastIncludeTerm)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf(111, "%v: refusing snapshot from leader %d 's snapshot request since its term is %d", rf.SayMeL(), args.LeaderId, args.Term)
		return
	}
	rf.state = Follower
	rf.resetElectionTimer()
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}
	defer rf.persist()

	if args.LastIncludeIndex > rf.snapshotLastIncludeIndex {
		DPrintf(800, "%v: before install snapshot from leader %d: leader.log=%v", rf.SayMeL(), args.LeaderId, rf.log)
		rf.snapshot = args.Snapshot
		rf.snapshotLastIncludeIndex = args.LastIncludeIndex
		rf.snapshotLastIncludeTerm = args.LastIncludeTerm
		if args.LastIncludeIndex >= rf.log.LastLogIndex {
			rf.log.Entries = make([]Entry, 0)
			rf.log.LastLogIndex = args.LastIncludeIndex
		} else {
			rf.log.Entries = rf.log.Entries[rf.log.getRealIndex(args.LastIncludeIndex+1):]
		}
		rf.log.FirstLogIndex = args.LastIncludeIndex + 1

		DPrintf(800, "%v: after install snapshot rf.log.FirstLogIndex=%v, rf.log=%v", rf.SayMeL(), rf.log.FirstLogIndex, rf.log)

		if args.LastIncludeIndex > rf.lastApplied {
			msg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.snapshotLastIncludeTerm,
				SnapshotIndex: rf.snapshotLastIncludeIndex,
			}
			DPrintf(800, "%v: next apply snapshot rf.snapshot.LastIncludeIndex=%v rf.snapshot.LastIncludeTerm=%v\n", rf.SayMeL(), rf.snapshotLastIncludeIndex, rf.snapshotLastIncludeTerm)
			rf.ApplyHelper.tryApply(&msg)
			rf.lastApplied = args.LastIncludeIndex
		}
		rf.commitIndex = max(rf.commitIndex, args.LastIncludeIndex)
	}
	DPrintf(111, "%v: successfully installing snapshot from LeaderId=%v with args.LastIncludeIndex=%v, args.LastIncludeTerm=%v\n", rf.SayMeL(), args.LeaderId, args.LastIncludeIndex, args.LastIncludeTerm)

}

func (rf *Raft) InstallSnapshot(serverId int) {

	args := RequestInstallSnapShotArgs{}
	reply := RequestInstallSnapShotReply{}
	rf.Mu.Lock()
	if rf.state != Leader {
		DPrintf(111, "%v: 状态已变，不是leader节点，无法发送快照", rf.SayMeL())
		rf.Mu.Unlock()
		return
	}
	DPrintf(111, "%v: 准备向节点%d发送快照", rf.SayMeL(), serverId)
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludeIndex = rf.snapshotLastIncludeIndex
	args.LastIncludeTerm = rf.snapshotLastIncludeTerm
	args.Snapshot = rf.snapshot
	rf.Mu.Unlock()

	ok := rf.sendRequestInstallSnapshot(serverId, &args, &reply)
	if !ok {
		//DPrintf(12, "%v: cannot sendRequestInstallSnapshot to  %v args.term=%v\n", rf.SayMeL(), serverId, args.Term)
		return
	}
	rf.Mu.Lock()
	defer rf.Mu.Unlock()

	if rf.state != Leader {
		DPrintf(111, "%v: 因为不是leader，放弃处理%d的快照响应", rf.SayMeL(), serverId)
		return
	}
	if reply.Term < rf.currentTerm {
		DPrintf(111, "%v: 因为是旧的快照响应，放弃处理%d的快照响应, 旧响应的任期是%d", rf.SayMeL(), serverId, reply.Term)
		return
	}
	if reply.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.persist()
		return
	}
	rf.peerTrackers[serverId].nextIndex = args.LastIncludeIndex + 1
	rf.peerTrackers[serverId].matchIndex = args.LastIncludeIndex
	DPrintf(111, "%v: 更新节点%d的nextIndex为%d, matchIndex为%d", rf.SayMeL(), serverId, rf.peerTrackers[serverId].nextIndex, args.LastIncludeIndex)

	rf.tryCommitL(rf.peerTrackers[serverId].matchIndex)
}

func (rf *Raft) sendRequestInstallSnapshot(server int, args *RequestInstallSnapShotArgs, reply *RequestInstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.RequestInstallSnapshot", args, reply)
	return ok
}
