package raft

import (
	"fmt"
	"math/rand"
	"time"
)

// let the base election timeout be T.
// the election timeout is in the range [T, 2T).
const baseElectionTimeout = 300
const None = -1

func (rf *Raft) StartElection() {
	rf.becomeCandidate()
	term := rf.currentTerm
	done := false
	votes := 1
	fmt.Printf("[%d] attempting an election at term %d...", rf.me, rf.currentTerm)
	args := RequestVoteArgs{rf.currentTerm, rf.me}

	for i, _ := range rf.peers {
		if rf.me == i {
			continue
		}
		// 开启协程去尝试拉选票
		go func(serverId int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(serverId, &args, &reply)
			//log.Printf("[%d] finish sending request vote to %d", rf.me, serverId)
			if !ok {
				DPrintf("%v: cannot give a Vote to %v args.term=%v\n", rf.SayMeL(), serverId, args.Term)
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
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
			fmt.Printf("\n[%d] got enough votes, and now is the leader(currentTerm=%d, state=%v)!\n", rf.me, rf.currentTerm, rf.state)
			rf.state = Leader           // 将自身设置为leader
			rf.StartAppendEntries(true) // 立即开始发送心跳而不是等定时器到期再发送，否则有一定概率在心跳到达从节点之前另一个leader也被选举成功，从而出现了两个leader
		}(i)
	}
}

func (rf *Raft) pastElectionTimeout() bool {
	return time.Since(rf.lastElection) > rf.electionTimeout
}

func (rf *Raft) resetElectionTimer() {
	electionTimeout := baseElectionTimeout + (rand.Int63() % baseElectionTimeout)
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.lastElection = time.Now()
}

func (rf *Raft) becomeFollower(term int) bool {
	rf.state = Follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = None
		return true
	}
	return false
}

func (rf *Raft) becomeCandidate() {
	//defer rf.persist()
	rf.state = Candidate
	rf.currentTerm++
	//rf.votedMe = make([]bool, len(rf.peers))
	rf.votedFor = rf.me
	rf.resetElectionTimer()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	//rf.resetTrackedIndexes()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	////fmt.Printf("[%d] begins grasping the lock...", rf.me)
	reply.VoteGranted = true    // 默认设置响应体为投同意票状态
	reply.Term = rf.currentTerm //
	//fmt.Printf("%v[RequestVote] from %v at args term: %v and current term: %v\n", args.CandidateId, rf.me, args.Term, rf.currentTerm)
	//竞选leader的节点任期小于等于自己的任期，则反对票(为什么等于情况也反对票呢？因为candidate节点在发送requestVote rpc之前会将自己的term+1)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = None
		rf.state = Follower
	}
	//Lab2B的日志复制直接确定为true
	update := true

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && update {
		//if rf.votedFor == -1 {
		//竞选任期大于自身任期，则更新自身任期，并转为follower
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.resetElectionTimer() //自己的票已经投出时就转为follower状态
	}
}
