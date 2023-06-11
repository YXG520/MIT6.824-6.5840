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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetElectionTimer()
	rf.becomeCandidate()
	done := false
	votes := 1
	term := rf.currentTerm
	DPrintf(111, "[%d] attempting an election at term %d...", rf.me, rf.currentTerm)
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
			//DPrintf(111, "%v: the reply term is %d and the voteGranted is %v", rf.SayMeL(), reply.Term, reply.VoteGranted)
			if !ok || !reply.VoteGranted {
				//DPrintf(111, "%v: cannot give a Vote to %v args.term=%v\n", rf.SayMeL(), serverId, args.Term)
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 丢弃无效票
			//if term != reply.Term {
			//	return
			//}
			if rf.currentTerm > reply.Term {
				return
			}
			// 统计票数
			votes++
			if done || votes <= len(rf.peers)/2 {
				// 在成为leader之前如果投票数不足需要继续收集选票
				// 同时在成为leader的那一刻，就不需要管剩余节点的响应了，因为已经具备成为leader的条件
				return
			}
			done = true
			if rf.state != Candidate || rf.currentTerm != term {
				return
			}
			DPrintf(111, "\n%v: [%d] got enough votes, and now is the leader(currentTerm=%d, state=%v)!\n", rf.SayMeL(), rf.me, rf.currentTerm, rf.state)
			rf.state = Leader // 将自身设置为leader
			DPrintf(111, "ready to send heartbeat to other nodes")
			go rf.StartAppendEntries(true) // 立即发送心跳

		}(i)
	}
}

func (rf *Raft) pastElectionTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	f := time.Since(rf.lastElection) > rf.electionTimeout
	return f
}

func (rf *Raft) resetElectionTimer() {
	//rf.mu.Lock()
	electionTimeout := baseElectionTimeout + (rand.Int63() % baseElectionTimeout)
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.lastElection = time.Now()
	DPrintf(111, "%v: 选举的超时时间设置为%d", rf.SayMeL(), rf.electionTimeout)
	//rf.mu.Unlock()

}

func (rf *Raft) becomeCandidate() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	DPrintf(111, "%v: 选举时间超时，将自身变为Candidate并且发起投票...", rf.SayMeL())

}
func (rf *Raft) ToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Follower
	rf.votedFor = None
	DPrintf(111, "%v: I am converting to a follower.", rf.SayMeL())
}

// example RequestVoteRPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	////fmt.Printf("[%d] begins grasping the lock...", rf.me)
	//fmt.Printf("%v[RequestVote] from %v at args term: %v and current term: %v\n", args.CandidateId, rf.me, args.Term, rf.currentTerm)
	//竞选leader的节点任期小于等于自己的任期，则反对票(为什么等于情况也反对票呢？因为candidate节点在发送requestVote rpc之前会将自己的term+1)
	if args.Term < rf.currentTerm {
		DPrintf(111, "%v: candidate的任期是%d, 小于我，所以拒绝", rf.SayMeL(), args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	DPrintf(111, "%v:candidate为%d,任期是%d", rf.SayMeL(), args.CandidateId, args.Term)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		//rf.mu.Unlock()
		rf.votedFor = None
		rf.state = Follower
		DPrintf(111, "%v:candidate %d的任期比自己大，所以修改rf.votedFor从%d到-1", rf.SayMeL(), args.CandidateId, rf.votedFor)
		//rf.ToFollower()
		//rf.mu.Lock()
	}
	reply.Term = rf.currentTerm
	//Lab2B的日志复制直接确定为true
	update := true
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && update {
		//竞选任期大于自身任期，则更新自身任期，并转为follower
		rf.votedFor = args.CandidateId
		rf.state = Follower
		//rf.mu.Unlock()
		rf.resetElectionTimer() //自己的票已经投出时就转为follower状态
		//electionTimeout := baseElectionTimeout + (rand.Int63() % baseElectionTimeout)
		//rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
		//rf.lastElection = time.Now()
		reply.VoteGranted = true // 默认设置响应体为投同意票状态
		DPrintf(111, "%v: 同意把票投给%d, 它的任期是%d", rf.SayMeL(), args.CandidateId, args.Term)
		//rf.mu.Lock()
	} else {
		reply.VoteGranted = false
		DPrintf(111, "%v:我已经投票给节点%d, 这次候选人的id为%d， 不符合要求，拒绝投票", rf.SayMeL(),
			rf.votedFor, args.CandidateId)
	}
	//rf.mu.Unlock()

}

// example RequestVoteRPC handler.
func (rf *Raft) RequestVote2(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	////fmt.Printf("[%d] begins grasping the lock...", rf.me)
	//fmt.Printf("%v[RequestVote] from %v at args term: %v and current term: %v\n", args.CandidateId, rf.me, args.Term, rf.currentTerm)
	//竞选leader的节点任期小于等于自己的任期，则反对票(为什么等于情况也反对票呢？因为candidate节点在发送requestVote rpc之前会将自己的term+1)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = None
		rf.state = Follower
		//rf.ToFollower()
	}
	reply.Term = rf.currentTerm
	//Lab2B的日志复制直接确定为true
	update := true
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && update {
		//if rf.votedFor == -1 {
		//竞选任期大于自身任期，则更新自身任期，并转为follower
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.resetElectionTimer()  //自己的票已经投出时就转为follower状态
		reply.VoteGranted = true // 默认设置响应体为投同意票状态
		DPrintf(111, "%v: 同意把票投给%d, 它的任期是%d", rf.SayMeL(), args.CandidateId, args.Term)
	} else {
		reply.VoteGranted = false
	}

}
