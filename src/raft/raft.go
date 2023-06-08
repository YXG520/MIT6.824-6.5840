package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"MIT6.824-6.5840/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"

import "MIT6.824-6.5840/labrpc"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 设置状态类型
const Follower, Candidate, Leader int = 1, 2, 3
const tickInterval = 50 * time.Millisecond
const heartbeatTimeout = 150 * time.Millisecond

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state            int           // 节点状态，Candidate-Follower-Leader
	currentTerm      int           // 当前的任期
	votedFor         int           // 投票给谁
	heartbeatTimeout time.Duration // 心跳定时器
	electionTimeout  time.Duration //选举计时器
	lastElection     time.Time     // 上一次的选举时间，用于配合since方法计算当前的选举时间是否超时
	lastHeartbeat    time.Time     // 上一次的心跳时间，用于配合since方法计算当前的心跳时间是否超时
	peerTrackers     []PeerTracker // Leader专属：keeps track of each peer's next index, match index, etc.
	log              *Log          // 日志记录

	//Volatile state
	commitIndex int // commitIndex是本机提交的
	lastApplied int // lastApplied是该日志在所有的机器上都跑了一遍后才会更新？

	applyHelper *ApplyHelper
	applyCond   *sync.Cond

	// attrs for producing snapshot
	snapshot                 []byte
	snapshotLastIncludeIndex int
	snapshotLastIncludeTerm  int
}

type RequestAppendEntriesArgs struct {
	LeaderTerm   int // Leader的Term
	LeaderId     int
	PrevLogIndex int // 新日志条目的上一个日志的索引
	PrevLogTerm  int // 新日志的上一个日志的任期
	//Logs         []ApplyMsg // 需要被保存的日志条目,可能有多个
	Entries      []Entry
	LeaderCommit int // Leader已提交的最高的日志项目的索引
}

type RequestAppendEntriesReply struct {
	FollowerTerm int  // Follower的Term,给Leader更新自己的Term
	Success      bool // 是否推送成功
	PrevLogIndex int
	PrevLogTerm  int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate’s term
	CandidateId int //candidate requesting vote

	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int //term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) getHeartbeatTime() time.Duration {
	return time.Millisecond * 110
}

// 随机化的选举超时时间
func (rf *Raft) getElectionTime() time.Duration {
	// [250,400) 250+[0,150]
	// return time.Millisecond * time.Duration(250+15*rf.me)
	//return time.Millisecond * time.Duration(350+rand.Intn(1000))

	return time.Millisecond * time.Duration(350+rand.Intn(200))
}

// 这个是只给tester调的
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {

	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm) // 持久化任期
	e.Encode(rf.votedFor)    // 持久化votedFor
	e.Encode(rf.log)         // 持久化日志
	data := w.Bytes()
	if rf.snapshotLastIncludeIndex > 0 {
		rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
	} else {
		rf.persister.SaveRaftState(data)
	}
	DPrintf(100, "%v: persist rf.currentTerm=%v rf.voteFor=%v rf.log=%v\n", rf.SayMeL(), rf.currentTerm, rf.votedFor, rf.log)
}

// restore previously persisted state.
func (rf *Raft) readPersist() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	stateData := rf.persister.ReadRaftState()
	if stateData == nil || len(stateData) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	if stateData != nil && len(stateData) > 0 { // bootstrap without any state?
		r := bytes.NewBuffer(stateData)
		d := labgob.NewDecoder(r)
		rf.votedFor = 0 // in case labgob waring
		if d.Decode(&rf.currentTerm) != nil ||
			d.Decode(&rf.votedFor) != nil ||
			d.Decode(&rf.log) != nil {
			//   error...
			DPrintf(999, "%v: readPersist decode error\n", rf.SayMeL())
			panic("")
		}
	}

	rf.snapshot = rf.persister.ReadSnapshot() // 这里持久化的时候只是将快照继续读取下来
	rf.commitIndex = rf.snapshotLastIncludeIndex
	rf.lastApplied = rf.snapshotLastIncludeIndex
	DPrintf(600, "%v: readPersist rf.currentTerm=%v rf.voteFor=%v rf.log=%v\n", rf.SayMeL(), rf.currentTerm, rf.votedFor, rf.log)

}

type RequestInstallSnapShotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Snapshot         []byte
}
type RequestInstallSnapShotReply struct {
	Term int
}

// example code to send a RequestVoteRPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//DPrintf("ready to call RequestVote Method...")
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendRequestAppendEntries(isHeartbeat bool, server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	var ok bool
	if isHeartbeat {
		ok = rf.peers[server].Call("Raft.HandleHeartbeatRPC", args, reply)
	} else {
		ok = rf.peers[server].Call("Raft.HandleAppendEntriesRPC", args, reply)
	}
	return ok
}
func (rf *Raft) sendRequestInstallSnapshot(server int, args *RequestInstallSnapShotArgs, reply *RequestInstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.HandleRequestInstallSnapshot", args, reply)
	return ok
}

// 从节点处理快照的逻辑
func (rf *Raft) HandleRequestInstallSnapshot(args *RequestInstallSnapShotArgs, reply *RequestInstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf(11, "%v: RequestInstallSnapshot end  args.LeaderId=%v, args.LastIncludeIndex=%v, args.LastIncludeTerm=%v\n", rf.SayMeL(), args.LeaderId, args.LastIncludeIndex, args.LastIncludeTerm)
	DPrintf(11, "%v: RequestInstallSnapshot begin  args.LeaderId=%v, args.LastIncludeIndex=%v, args.LastIncludeTerm=%v\n", rf.SayMeL(), args.LeaderId, args.LastIncludeIndex, args.LastIncludeTerm)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.state = Follower
	//rf.electionTimer.Reset(rf.getElectionTime())
	rf.resetElectionTimer()

	if args.Term > rf.currentTerm {
		rf.NewTermL(args.Term)
	}
	defer rf.persist()
	// 如果leader发送过来的日志索引大于从节点自身存的快照索引，则需要更新
	if args.LastIncludeIndex > rf.snapshotLastIncludeIndex {
		// DPrintf(800, "%v: before install snapshot %s: rf.log.FirstLogIndex=%v, rf.log=%v", rf.SayMeL(), rf.SayMeL(), rf.log.FirstLogIndex, rf.log)
		rf.snapshot = args.Snapshot
		rf.snapshotLastIncludeIndex = args.LastIncludeIndex
		rf.snapshotLastIncludeTerm = args.LastIncludeTerm
		if args.LastIncludeIndex >= rf.log.LastLogIndex {
			// 如果leader备份的index超过了自己的最后一项日志索引，则直接将日志节点置空即可
			rf.log.Entries = make([]Entry, 0)
			rf.log.LastLogIndex = args.LastIncludeIndex
		} else {
			// 否则截断直到args.LastIncludeIndex的日志
			rf.log.Entries = rf.log.Entries[rf.log.getRealIndex(args.LastIncludeIndex+1):]
		}
		// 这个起始索引也会发生改变
		rf.log.FirstLogIndex = args.LastIncludeIndex + 1

		// DPrintf(800, "%v: after install snapshot rf.log.FirstLogIndex=%v, rf.log=%v", rf.SayMeL(), rf.log.FirstLogIndex, rf.log)
		// 如果leader快照所在的日志索引大于从节点的rf.lastApplied的索引，则可以也可以让从节点的状态机应用此快照
		if args.LastIncludeIndex > rf.lastApplied {
			msg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.snapshotLastIncludeTerm,
				SnapshotIndex: rf.snapshotLastIncludeIndex,
			}
			DPrintf(800, "%v: next apply snapshot rf.snapshot.LastIncludeIndex=%v rf.snapshot.LastIncludeTerm=%v\n", rf.SayMeL(), rf.snapshotLastIncludeIndex, rf.snapshotLastIncludeTerm)
			rf.applyHelper.tryApply(&msg)
			rf.lastApplied = args.LastIncludeIndex
		}
		// 更新commitIndex
		rf.commitIndex = max(rf.commitIndex, args.LastIncludeIndex)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 这个方法只供测试使用，大概每一个raft实例都会开启一个start协程，然后去尝试给集群中的节点发送日志，但是只有Leader节点能成功发送rpc
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state != Leader {
		isLeader = false
		return index, term, isLeader
	}
	index = rf.log.LastLogIndex + 1
	// 开始发送AppendEntries rpc

	DPrintf(100, "%v: a command index=%v cmd=%T %v come", rf.SayMeL(), index, command, command)
	rf.log.appendL(Entry{term, command})
	rf.persist()
	//rf.resetTrackedIndex()
	DPrintf(101, "%v: check the newly added log index：%d", rf.SayMeL(), rf.log.LastLogIndex)
	//go rf.StartAppendEntries(false)
	rf.StartAppendEntries(false)

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf(111, "%v : is killed!!", rf.SayMeL())

	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.applyHelper.Kill()
	DPrintf(111, "%v : my applyHelper is killed!!", rf.SayMeL())

	rf.state = Follower
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead) // 这里的kill仅仅将对应的字段置为1
	return z == 1
}

// leader发送快照的逻辑
func (rf *Raft) InstallSnapshot(serverId int) {

	args := RequestInstallSnapShotArgs{}
	reply := RequestInstallSnapShotReply{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(11, "%v: InstallSnapshot begin serverId=%v myinfo:rf.lastApplied=%v, rf.log.FirstLogIndex=%v\n", rf.SayMeL(), serverId, rf.lastApplied, rf.log.FirstLogIndex)
	defer DPrintf(11, "%v: InstallSnapshot end serverId=%v\n", rf.SayMeL(), serverId)
	if rf.state != Leader {
		return
	}
	// if rf.lastApplied < rf.log.FirstLogIndex {
	// 	return
	// }
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludeIndex = rf.snapshotLastIncludeIndex
	args.LastIncludeTerm = rf.snapshotLastIncludeTerm
	args.Snapshot = rf.snapshot
	rf.mu.Unlock()
	ok := rf.sendRequestInstallSnapshot(serverId, &args, &reply)
	rf.mu.Lock()
	if !ok {
		DPrintf(12, "%v: cannot sendRequestInstallSnapshot to  %v args.term=%v\n", rf.SayMeL(), serverId, args.Term)
		return
	}
	if rf.state != Leader {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeLeader()
		rf.NewTermL(reply.Term)
		rf.persist()
		return
	}
	rf.peerTrackers[serverId].nextIndex = args.LastIncludeIndex + 1
	rf.peerTrackers[serverId].matchIndex = args.LastIncludeIndex
	rf.tryCommitL(rf.peerTrackers[serverId].matchIndex)
}

// // the service says it has created a snapshot that has
// // all info up to and including index. this means the
// // service no longer needs the log through (and including)
// // that index. Raft should now trim its log as much as possible.
// 两个参数，一个是日志的index，一个是持久化后的snapshot字节数组, 这里的index其实就是lastIncludedIndex
// 这个方法应该就是发生故障重启后进行快照恢复的执行过程
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	DPrintf(111, "begin installSnapshot:")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// leader才能负责讲快照分发给从节点
	if rf.state != Leader {
		return
	}
	DPrintf(111, "%v: come Snapshot index=%v", rf.SayMeL(), index)
	// 确保日志索引没有越界
	if rf.log.FirstLogIndex <= index {
		if index > rf.lastApplied {
			panic(fmt.Sprintf("%v: index=%v rf.lastApplied=%v\n", rf.SayMeL(), index, rf.lastApplied))
		}
		// leader先更新自己的快照，以及快照对应的日志索引和任期
		rf.snapshot = snapshot
		rf.snapshotLastIncludeIndex = index
		rf.snapshotLastIncludeTerm = rf.getEntryTerm(index)

		// 因为一旦使用了快照，就需要删除前面的所有日志，物理上的日志开始索引变成了当前的快照索引+1
		newFirstLogIndex := index + 1
		if newFirstLogIndex <= rf.log.LastLogIndex {
			// 截取从newFirstLogIndex开始的子日志 等价于删除包括index及其之前的日志
			rf.log.Entries = rf.log.Entries[newFirstLogIndex-rf.log.FirstLogIndex:]
		} else {
			// 如果新日志索引比当前日志中的索引上界还大，则更新上界值为lastIncludedIndex
			// 这种情况只会发生在日志为空的情况下，因为日志为空，LastLogIndex=0
			rf.log.LastLogIndex = newFirstLogIndex - 1
			rf.log.Entries = make([]Entry, 0)
		}
		rf.log.FirstLogIndex = newFirstLogIndex
		// 如果一个leader崩溃，新leader被选出时，其commitIndex很有可能是落后于旧leader的commitIndex
		// 因为从节点的commitIndex取决于leader的commitIndex
		rf.commitIndex = max(rf.commitIndex, index)
		// lastApplied字段永远小于等于commitIndex，所以
		// 新leader被选出时，其lastApplied很有可能是落后于旧leader的lastApplied
		rf.lastApplied = max(rf.lastApplied, index)
		DPrintf(111, "%v:进行快照后，更新commitIndex为%d, lastApplied为%d", rf.SayMeL(), rf.commitIndex, rf.lastApplied)
		rf.persist() // 将其持久化
		// 将快照发送给所有从节点
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.InstallSnapshot(i)
		}
		DPrintf(11, "%v: len(rf.log.Entries)=%v rf.log.FirstLogIndex=%v rf.log.LastLogIndex=%v rf.commitIndex=%v  rf.lastApplied=%v\n",
			rf.SayMeL(), len(rf.log.Entries), rf.log.FirstLogIndex, rf.log.LastLogIndex, rf.commitIndex, rf.lastApplied)
	}
}

func (rf *Raft) StartAppendEntries(heart bool) {
	// 并行向其他节点发送心跳或者日志，让他们知道此刻已经有一个leader产生
	//DPrintf(111, "%v: detect the len of peers: %d", rf.SayMeL(), len(rf.peers))
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		//go rf.AppendEntries(i, heart)
		go rf.AppendEntries(i, heart)

	}
}

// nextIndex收敛速度优化：nextIndex跳跃算法，需搭配HandleAppendEntriesRPC2方法使用
func (rf *Raft) AppendEntries(targetServerId int, heart bool) {
	rf.mu.Lock()
	rf.resetElectionTimer()
	rf.mu.Unlock()

	if heart {
		rf.mu.Lock()
		reply := RequestAppendEntriesReply{}
		args := RequestAppendEntriesArgs{}
		args.LeaderTerm = rf.currentTerm
		DPrintf(111, "\n %d is a leader with term %d, ready sending heartbeart to follower %d....", rf.me, rf.currentTerm, targetServerId)
		rf.mu.Unlock()

		rf.sendRequestAppendEntries(true, targetServerId, &args, &reply)
		// 发送心跳包
		return
	} else {
		args := RequestAppendEntriesArgs{}
		rf.mu.Lock()

		args.PrevLogIndex = min(rf.log.LastLogIndex, rf.peerTrackers[targetServerId].nextIndex-1)
		if args.PrevLogIndex+1 < rf.log.FirstLogIndex {
			DPrintf(111, "此时 %d 节点的nextIndex为%d,LastLogIndex为 %d, 最后一项日志为：\n", rf.me, rf.peerTrackers[rf.me].nextIndex,
				rf.log.LastLogIndex)
			go rf.InstallSnapshot(targetServerId)
			return
		}

		args.LeaderTerm = rf.currentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex

		data := rf.log.getAppendEntries(args.PrevLogIndex + 1)
		term := rf.getEntryTerm(args.PrevLogIndex)
		DPrintf(111, "%v: ready to send entries to %d with rf.log.FirstLogIndex %d rf.log.LastLogIndex %d, rf.peerTrackers[targetServerId].nextIndex-1 %d, and "+
			"\n args.PrevLogIndex %d. \n The lastIncludeIndex is %d and lastIncludeTerm is %d. \n The Entries' len is %d and contents are as follow: %v."+
			" \n The append entries' len is %d and contents are as follow: %v",
			rf.SayMeL(), targetServerId, rf.log.FirstLogIndex, rf.log.LastLogIndex,
			rf.peerTrackers[targetServerId].nextIndex-1, args.PrevLogIndex, rf.snapshotLastIncludeIndex, rf.snapshotLastIncludeTerm,
			len(rf.log.Entries), rf.log.Entries, len(data), data)
		args.Entries = data
		args.PrevLogTerm = term
		reply := RequestAppendEntriesReply{}
		DPrintf(111, "%v: the len of append log entries: %d is ready to send to node %d!!! and the entries are %v\n",
			rf.SayMeL(), len(args.Entries), targetServerId, args.Entries)
		rf.mu.Unlock()

		ok := rf.sendRequestAppendEntries(false, targetServerId, &args, &reply)
		if !ok {
			//DPrintf(111, "%v: cannot request AppendEntries to %v args.term=%v\n", rf.SayMeL(), targetServerId, args.LeaderTerm)
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()

		DPrintf(111, "%v: get reply from %v reply.Term=%v reply.Success=%v reply.PrevLogTerm=%v reply.PrevLogIndex=%v myinfo:rf.log.FirstLogIndex=%v rf.log.LastLogIndex=%v\n",
			rf.SayMeL(), targetServerId, reply.FollowerTerm, reply.Success, reply.PrevLogTerm, reply.PrevLogIndex, rf.log.FirstLogIndex, rf.log.LastLogIndex)
		if reply.FollowerTerm > rf.currentTerm {
			rf.state = Follower
			rf.NewTermL(reply.FollowerTerm)
			rf.persist()
			return
		}
		DPrintf(111, "%v: get append reply reply.PrevLogIndex=%v reply.PrevLogTerm=%v reply.Success=%v heart=%v\n", rf.SayMeL(), reply.PrevLogIndex, reply.PrevLogTerm, reply.Success, heart)

		if reply.Success {
			rf.peerTrackers[targetServerId].nextIndex = args.PrevLogIndex + len(args.Entries) + 1
			rf.peerTrackers[targetServerId].matchIndex = args.PrevLogIndex + len(args.Entries)
			DPrintf(111, "success! now trying to commit the log...\n")
			rf.tryCommitL(rf.peerTrackers[targetServerId].matchIndex)
			return
		}

		//reply.Success is false
		// leader自己的日志被清空，说明进行了一次快照，需要传递给从节点
		if rf.log.empty() { //
			go rf.InstallSnapshot(targetServerId)
			return
		}
		// 如果从节点的最后一个日志项索引赶不上
		// 或者刚好赶上leader的最近一次快照的日志索引
		// 则直接给从节点发送这个快照
		if reply.PrevLogIndex+1 < rf.log.FirstLogIndex {
			go rf.InstallSnapshot(targetServerId)
			return
		}

		if reply.PrevLogIndex > rf.log.LastLogIndex {
			rf.peerTrackers[targetServerId].nextIndex = rf.log.LastLogIndex + 1
		} else if rf.getEntryTerm(reply.PrevLogIndex) == reply.PrevLogTerm {
			// 因为响应方面接收方做了优化，作为响应方的从节点可以直接跳到索引不匹配但是等于任期PrevLogTerm的第一个提交的日志记录
			rf.peerTrackers[targetServerId].nextIndex = reply.PrevLogIndex + 1
		} else {
			// 此时rf.getEntryTerm(reply.PrevLogIndex) != reply.PrevLogTerm，也就是说此时索引相同位置上的日志提交时所处term都不同，
			// 则此日志也必然是不同的，所以可以安排跳到前一个当前任期的第一个节点
			PrevIndex := reply.PrevLogIndex
			for PrevIndex >= rf.log.FirstLogIndex && rf.getEntryTerm(PrevIndex) == rf.getEntryTerm(reply.PrevLogIndex) {
				PrevIndex--
			}
			if PrevIndex+1 < rf.log.FirstLogIndex {
				if rf.log.FirstLogIndex > 1 {
					go rf.InstallSnapshot(targetServerId)
					return
				}
			}
			rf.peerTrackers[targetServerId].nextIndex = PrevIndex + 1
		}
		DPrintf(3, "%v in AppendEntries change nextindex[%v]=%v and retry\n",
			rf.SayMeL(), targetServerId, rf.peerTrackers[targetServerId].nextIndex)
		//go rf.AppendEntries(targetServerId, false)
	}
}

func (rf *Raft) NewTermL(term int) {
	DPrintf(850, "%v : from old term %v to new term %v\n", rf.SayMeL(), rf.currentTerm, term)
	rf.currentTerm, rf.votedFor = term, None
}

func (rf *Raft) SayMeL() string {
	//return fmt.Sprintf("[Server %v as %v at term %v]", rf.me, rf.state, rf.currentTerm)
	return "success"
}

// 通知tester接收这个日志消息，然后供测试使用
// 这里的tester相当于状态机
func (rf *Raft) sendMsgToTester() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		DPrintf(110, "%v: it is being blocked...", rf.SayMeL())
		rf.applyCond.Wait()

		for rf.lastApplied+1 <= rf.commitIndex {
			i := rf.lastApplied + 1
			rf.lastApplied++
			if i < rf.log.FirstLogIndex {
				DPrintf(111, "%v: apply index=%v but rf.log.FirstLogIndex=%v rf.lastApplied=%v\n",
					rf.SayMeL(), i, rf.log.FirstLogIndex, rf.lastApplied)
				panic("error happening")
			}
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.getOneEntry(i).Command,
				CommandIndex: i,
			}
			DPrintf(111, "%s: next apply index=%v lastApplied=%v len entries=%v "+
				"LastLogIndex=%v cmd=%v\n", rf.SayMeL(), i, rf.lastApplied, len(rf.log.Entries),
				rf.log.LastLogIndex, rf.log.getOneEntry(i).Command)
			rf.applyHelper.tryApply(&msg)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = None
	rf.state = Follower //设置节点的初始状态为follower
	rf.resetElectionTimer()
	rf.heartbeatTimeout = heartbeatTimeout // 这个是固定的
	rf.log = NewLog()
	rf.snapshot = nil
	rf.snapshotLastIncludeIndex = 0
	rf.snapshotLastIncludeTerm = 0
	// initialize from state persisted before a crash
	rf.readPersist()
	// 必须在持久化之后才能调用,
	rf.applyHelper = NewApplyHelper(applyCh, rf.lastApplied)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.peerTrackers = make([]PeerTracker, len(rf.peers)) //对等节点追踪器
	rf.applyCond = sync.NewCond(&rf.mu)

	//Leader选举协程
	go rf.ticker()
	go rf.sendMsgToTester() // 供config协程追踪日志以测试

	return rf
}

func (rf *Raft) ticker() {
	// 如果这个raft节点没有掉线,则一直保持活跃不下线状态（可以因为网络原因掉线，也可以tester主动让其掉线以便测试）
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Follower:
			//DPrintf(111, "I am %d, a follower with term %d and my dead state is %d", rf.me, rf.currentTerm, rf.dead)
			fallthrough // 相当于执行#A到#C代码块,
			//if rf.pastElectionTimeout() {
			//	rf.StartElection()
			//}
		case Candidate:
			//DPrintf(111, "I am %d, a Candidate with term %d and my dead state is %d", rf.me, rf.currentTerm, rf.dead)
			if rf.pastElectionTimeout() { //#A
				rf.StartElection()
			} //#C

		case Leader:

			// 只有Leader节点才能发送心跳和日志给从节点
			isHeartbeat := false
			// 检测是需要发送单纯的心跳还是发送日志
			// 心跳定时器过期则发送心跳，否则发送日志
			if rf.pastHeartbeatTimeout() {
				isHeartbeat = true
				rf.resetHeartbeatTimer()
			}
			rf.StartAppendEntries(isHeartbeat)
		}

		time.Sleep(tickInterval)
	}
	DPrintf(111, "tim")
}

func (rf *Raft) getLastEntryTerm() int {
	if rf.log.LastLogIndex >= rf.log.FirstLogIndex {
		return rf.log.getOneEntry(rf.log.LastLogIndex).Term
	}
	//else {
	//	return rf.snapshotLastIncludeTerm
	//}
	return -1

}
