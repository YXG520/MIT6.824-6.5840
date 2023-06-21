package raft

import (
	"math/rand"
	"time"
)

// let the base election timeout be T.
// the election timeout is in the range [T, 2T).
const baseElectionTimeout = 300
const None = -1

func (rf *Raft) StartElection() {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	rf.becomeCandidate()
	term := rf.currentTerm
	done := false
	votes := 1
	DPrintf(222, "[%d] attempting an election at term %d...", rf.me, rf.currentTerm)
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.log.LastLogIndex
	args.LastLogTerm = rf.getLastEntryTerm()
	defer rf.persist()
	for i, _ := range rf.peers {
		if rf.me == i {
			continue
		}
		// 开启协程去尝试拉选票
		go func(serverId int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(serverId, &args, &reply)
			//log.Printf("[%d] finish sending request vote to %d", rf.me, serverId)
			if !ok || !reply.VoteGranted {
				//DPrintf(101, "%v: cannot be given a vote by node %v at reply.term=%v\n", rf.SayMeL(), serverId, reply.Term)
				return
			}

			rf.Mu.Lock()
			defer rf.Mu.Unlock()
			DPrintf(101, "%v: now receiving a vote from %d with term %d", rf.SayMeL(), serverId, reply.Term)

			if reply.Term < rf.currentTerm {
				DPrintf(111, "%v: 来自%d 在任期 %d 的旧投票，拒绝接受", rf.SayMeL(), serverId, reply.Term)
				return
			}
			// 角色变换
			if reply.Term > rf.currentTerm {
				DPrintf(111, "%v: %d 的任期是 %d, 比我大，变为follower", rf.SayMeL(), serverId, args.Term)
				rf.state = Follower
				rf.votedFor = None
				rf.currentTerm = reply.Term
				rf.persist()
				return
			}
			// 统计票数
			votes++
			if done || votes <= len(rf.peers)/2 {
				// 在成为leader之前如果投票数不足需要继续收集选票
				// 同时在成为leader的那一刻，就不需要管剩余节点的响应了，因为已经具备成为leader的条件
				return
			}
			if rf.state != Candidate || rf.currentTerm != term {
				return
			}
			//rf.state = Leader // 将自身设置为leader
			rf.becomeLeader()
			DPrintf(222, "\n%v: [%d] got enough votes, and now is the leader(currentTerm=%d, state=%v)!starting to append heartbeat...\n", rf.SayMeL(), rf.me, rf.currentTerm, rf.state)
			go rf.StartAppendEntries(true) // 立即开始发送心跳而不是等定时器到期再发送，否则有一定概率在心跳到达从节点之前另一个leader也被选举成功，从而出现了两个leader
		}(i)
	}
}

func (rf *Raft) pastElectionTimeout() bool {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	return time.Since(rf.lastElection) > rf.electionTimeout
}

func (rf *Raft) resetElectionTimer() {
	electionTimeout := baseElectionTimeout + (rand.Int63() % baseElectionTimeout)
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.lastElection = time.Now()
	DPrintf(222, "%d has refreshed the electionTimeout at term %d to a random value %d...\n", rf.me, rf.currentTerm, rf.electionTimeout/1000000)
}

func (rf *Raft) becomeCandidate() {
	rf.resetElectionTimer()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	DPrintf(100, "%v :becomes leader and reset TrackedIndex\n", rf.SayMeL())
	rf.resetTrackedIndex()

}

// 定义一个心跳兼日志同步处理器，这个方法是Candidate和Follower节点的处理
func (rf *Raft) HandleHeartbeatRPC(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.Mu.Lock() // 加接收心跳方的锁
	defer rf.Mu.Unlock()
	reply.FollowerTerm = rf.currentTerm
	reply.Success = true
	// 旧任期的leader抛弃掉
	if args.LeaderTerm < rf.currentTerm {
		reply.Success = false
		return
	}
	//DPrintf(200, "I am %d and the dead state is %d with term %d", rf.me)
	//DPrintf(200, "%v: I am now receiving heartbeat from leader %d and dead state is %d", rf.SayMeL(), args.LeaderId, rf.dead)
	rf.resetElectionTimer()
	// 需要转变自己的身份为Follower
	rf.state = Follower
	// 承认来者是个合法的新leader，则任期一定大于自己，此时需要设置votedFor为-1以及
	if args.LeaderTerm > rf.currentTerm {
		rf.votedFor = None
		rf.currentTerm = args.LeaderTerm
		reply.FollowerTerm = rf.currentTerm
		//rf.persist()
	}
	rf.persist()

	// 重置自身的选举定时器，这样自己就不会重新发出选举需求（因为它在ticker函数中被阻塞住了）
}

// example RequestVoteRPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	reply.VoteGranted = true // 默认设置响应体为投同意票状态
	reply.Term = rf.currentTerm
	//竞选leader的节点任期小于等于自己的任期，则反对票(为什么等于情况也反对票呢？因为candidate节点在发送requestVote rpc之前会将自己的term+1)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.persist()
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = None
		rf.state = Follower
		reply.Term = rf.currentTerm
		rf.persist()
	}
	DPrintf(500, "%v: reply to %v myLastLogterm=%v myLastLogIndex=%v args.LastLogTerm=%v args.LastLogIndex=%v\n",
		rf.SayMeL(), args.CandidateId, rf.getLastEntryTerm(), rf.log.LastLogIndex, args.LastLogTerm, args.LastLogIndex)

	// candidate节点发送过来的日志索引以及任期必须大于等于自己的日志索引及任期
	update := false
	update = update || args.LastLogTerm > rf.getLastEntryTerm()
	update = update || args.LastLogTerm == rf.getLastEntryTerm() && args.LastLogIndex >= rf.log.LastLogIndex

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && update {
		//if rf.votedFor == -1 {
		//竞选任期大于自身任期，则更新自身任期，并转为follower
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.resetElectionTimer() //自己的票已经投出时就转为follower状态
		DPrintf(111, "%v: 投出同意票给节点%d", rf.SayMeL(), args.CandidateId)
		rf.persist()

	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf(111, "%v: 投出反对票给节点%d", rf.SayMeL(), args.CandidateId)
	}

}
