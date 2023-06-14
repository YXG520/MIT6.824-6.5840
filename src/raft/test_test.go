package raft

//
// Raft tests.
//
// we will use the original test_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "testing"
import "fmt"
import "time"
import "math/rand"
import "sync/atomic"
import "sync"

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const RaftElectionTimeout = 1000 * time.Millisecond

func TestInitialElection2A(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): initial election")

	// is a leader elected?
	cfg.checkOneLeader()

	// sleep a bit to avoid racing with followers learning of the
	// election, then check that all peers agree on the term.
	time.Sleep(50 * time.Millisecond)
	term1 := cfg.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %v, but should be at least 1", term1)
	}

	// does the leader+term stay the same if there is no network failure?
	time.Sleep(2 * RaftElectionTimeout)
	term2 := cfg.checkTerms()
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	// there should still be a leader.
	cfg.checkOneLeader()

	cfg.end()
	fmt.Printf("test1 finished...")
}

func TestReElection2A(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): election after network failure")

	leader1 := cfg.checkOneLeader()

	// if the leader disconnects, a new one should be elected.
	cfg.disconnect(leader1)
	cfg.checkOneLeader()

	// if the old leader rejoins, that shouldn't
	// disturb the new leader. and the old leader
	// should switch to follower.
	cfg.connect(leader1)
	leader2 := cfg.checkOneLeader()

	// if there's no quorum, no new leader should
	// be elected.
	cfg.disconnect(leader2)
	cfg.disconnect((leader2 + 1) % servers)
	time.Sleep(2 * RaftElectionTimeout)

	// check that the one connected server
	// does not think it is the leader.
	cfg.checkNoLeader()

	// if a quorum arises, it should elect a leader.
	cfg.connect((leader2 + 1) % servers)
	cfg.checkOneLeader()

	// re-join of last node shouldn't prevent leader from existing.
	cfg.connect(leader2)
	cfg.checkOneLeader()

	cfg.end()
}

func TestManyElections2A(t *testing.T) {
	servers := 7
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): multiple elections")

	cfg.checkOneLeader()

	iters := 10
	for ii := 1; ii < iters; ii++ {
		DPrintf(110, "the  %d th iter...\n", ii)
		// disconnect three nodes
		i1 := rand.Int() % servers
		i2 := rand.Int() % servers
		i3 := rand.Int() % servers
		cfg.disconnect(i1)
		cfg.disconnect(i2)
		cfg.disconnect(i3)
		// either the current leader should still be alive,
		// or the remaining four should elect a new one.
		cfg.checkOneLeader()

		cfg.connect(i1)
		cfg.connect(i2)
		cfg.connect(i3)
	}

	cfg.checkOneLeader()

	cfg.end()
}

func TestBasicAgree2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B1): basic agreement")
	iters := 3
	// 测试iters次
	for index := 1; index < iters+1; index++ {
		DPrintf(11, "\nthis is the %d th iter...\n", index)
		//检测start函数调用前，有几个节点已经提交了日志，因为调用start函数就相当于tester
		//生成日志项并且投放给主节点，所以这里的没有启动start方法就没有日志项产生
		nd, _ := cfg.nCommitted(index)
		if nd > 0 {
			t.Fatalf("some have committed before Start()")
		}
		// 检查索引大多数节点对待同一个日志，存储的位置（即索引）是否和生产时的顺序相同
		xindex := cfg.one(index*100, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
		DPrintf(11, "\nfinished the %d th iter...\n", index)

	}
	cfg.end()
	DPrintf(11, "testB1 finished...")
}

// check, based on counting bytes of RPCs, that
// each command is sent to each peer just once.

func TestRPCBytes2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B2): RPC byte count")

	cfg.one(99, servers, false)
	bytes0 := cfg.bytesTotal()

	iters := 3
	var sent int64 = 0
	for index := 2; index < iters+2; index++ {
		cmd := randstring(5000)
		xindex := cfg.one(cmd, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
		sent += int64(len(cmd))
	}

	bytes1 := cfg.bytesTotal()
	got := bytes1 - bytes0
	expected := int64(servers) * sent
	if got > expected+50000 {
		t.Fatalf("too many RPC bytes; got %v, expected %v", got, expected)
	}

	cfg.end()
}

// test just failure of followers.
func TestFollowerFailure2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B3): test progressive failure of followers")

	cfg.one(101, servers, false)

	// disconnect one follower from the network.
	leader1 := cfg.checkOneLeader()
	cfg.disconnect((leader1 + 1) % servers)

	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	cfg.one(102, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	cfg.one(103, servers-1, false)

	// disconnect the remaining follower
	leader2 := cfg.checkOneLeader()
	cfg.disconnect((leader2 + 1) % servers)
	cfg.disconnect((leader2 + 2) % servers)

	// submit a command.
	index, _, ok := cfg.rafts[leader2].Start(104)
	if ok != true {
		t.Fatalf("leader rejected Start()")
	}
	if index != 4 {
		t.Fatalf("expected index 4, got %v", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	// check that command 104 did not commit.
	n, _ := cfg.nCommitted(index)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	cfg.end()
}

// test just failure of leaders.
func TestLeaderFailure2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B4): test failure of leaders")

	cfg.one(101, servers, false)

	// disconnect the first leader.
	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)

	// the remaining followers should elect
	// a new leader.
	cfg.one(102, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	cfg.one(103, servers-1, false)

	// disconnect the new leader.
	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)

	// submit a command to each server.
	for i := 0; i < servers; i++ {
		cfg.rafts[i].Start(104)
	}

	time.Sleep(2 * RaftElectionTimeout)

	// check that command 104 did not commit.
	n, _ := cfg.nCommitted(4)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	cfg.end()
}

// test that a follower participates after
// disconnect and re-connect.
func TestFailAgree2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B5): agreement after follower reconnects")

	cfg.one(101, servers, false)

	// disconnect one follower from the network.
	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % servers)
	DPrintf(111, "already let the node %d offline to add new entries...", (leader+1)%servers)
	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	cfg.one(102, servers-1, false)
	cfg.one(103, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	cfg.one(104, servers-1, false)
	cfg.one(105, servers-1, false)
	// re-connect
	cfg.connect((leader + 1) % servers)
	DPrintf(111, "after connected, check whether previous added entries can sync to %d...", (leader+1)%servers)

	// the full set of servers should preserve
	// previous agreements, and be able to agree
	// on new commands.
	cfg.one(106, servers, true)
	time.Sleep(RaftElectionTimeout)
	cfg.one(107, servers, true)

	cfg.end()
}

func TestFailNoAgree2B(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B6): no agreement if too many followers disconnect")

	cfg.one(10, servers, false)

	// 3 of 5 followers disconnect
	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % servers)
	cfg.disconnect((leader + 2) % servers)
	cfg.disconnect((leader + 3) % servers)

	index, _, ok := cfg.rafts[leader].Start(20)
	if ok != true {
		t.Fatalf("leader rejected Start()")
	}
	if index != 2 {
		t.Fatalf("expected index 2, got %v", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	n, _ := cfg.nCommitted(index)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	// repair
	cfg.connect((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.connect((leader + 3) % servers)

	// the disconnected majority may have chosen a leader from
	// among their own ranks, forgetting index 2.
	leader2 := cfg.checkOneLeader()
	index2, _, ok2 := cfg.rafts[leader2].Start(30)
	if ok2 == false {
		t.Fatalf("leader2 rejected Start()")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %v", index2)
	}

	cfg.one(1000, servers, true)

	cfg.end()
}

func TestConcurrentStarts2B(t *testing.T) {
	servers := 7
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B7): concurrent Start()s")

	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader := cfg.checkOneLeader()
		_, term, ok := cfg.rafts[leader].Start(1)
		if !ok {
			// leader moved on really quickly
			continue
		}

		iters := 5
		var wg sync.WaitGroup
		is := make(chan int, iters)
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				i, term1, ok := cfg.rafts[leader].Start(100 + i)
				if term1 != term {
					return
				}
				if ok != true {
					return
				}
				is <- i
			}(ii)
		}

		wg.Wait()
		close(is)

		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); t != term {
				// term changed -- can't expect low RPC counts
				continue loop
			}
		}

		failed := false
		cmds := []int{}
		for index := range is {
			cmd := cfg.wait(index, servers, term)
			if ix, ok := cmd.(int); ok {
				if ix == -1 {
					// peers have moved on to later terms
					// so we can't expect all Start()s to
					// have succeeded
					failed = true
					break
				}
				cmds = append(cmds, ix)
			} else {
				t.Fatalf("value %v is not an int", cmd)
			}
		}

		if failed {
			// avoid leaking goroutines
			go func() {
				for range is {
				}
			}()
			continue
		}

		for ii := 0; ii < iters; ii++ {
			x := 100 + ii
			ok := false
			for j := 0; j < len(cmds); j++ {
				if cmds[j] == x {
					ok = true
				}
			}
			if ok == false {
				t.Fatalf("cmd %v missing in %v", x, cmds)
			}
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	cfg.end()
}

func TestRejoin2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B8): rejoin of partitioned leader")

	cfg.one(101, servers, true)

	// leader network failure
	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)

	// make old leader try to agree on some entries
	cfg.rafts[leader1].Start(102)
	cfg.rafts[leader1].Start(103)
	cfg.rafts[leader1].Start(104)

	// new leader commits, also for index=2
	cfg.one(103, 2, true)

	// new leader network failure
	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)

	// old leader connected again
	cfg.connect(leader1)

	cfg.one(104, 2, true)

	// all together now
	cfg.connect(leader2)

	cfg.one(105, servers, true)

	cfg.end()
}

func TestBackup2B(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B9): leader backs up quickly over incorrect follower logs")

	cfg.one(rand.Int(), servers, true)

	// put leader and one follower in a partition
	leader1 := cfg.checkOneLeader()
	DPrintf(11, "check one leader alive with id %d...\n", cfg.rafts[leader1].me)
	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)
	DPrintf(11, "let followers whose id is greater than leader %d were forced offline...\n", cfg.rafts[leader1].me)

	// submit lots of commands that won't commit
	// 意思是因为前面强制下线了多数节点，所以这里的添加的日志都会丢失，不会被提交
	for i := 0; i < 50; i++ {
		cfg.rafts[leader1].Start(rand.Int())
	}
	DPrintf(11, "after corruption, 50 cmds are inserted and it should not be successfully done...\n")
	time.Sleep(RaftElectionTimeout / 2)

	cfg.disconnect((leader1 + 0) % servers)
	cfg.disconnect((leader1 + 1) % servers)
	DPrintf(11, "last 2 instances are done now !!!！\n")
	// allow other partition to recover
	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)
	DPrintf(11, "3 nodes are now reconnected and new 50 cmds is gonna be checked whether be refused...\n")
	// lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}
	//DPrintf(11, "%v : refuse all, good!\n", cfg.rafts[leader1].SayMeL())
	// now another partitioned leader and one follower
	leader2 := cfg.checkOneLeader()
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers // 另一个从节点
	}
	//DPrintf(11, "%v : checkOneLeader!!!\n", cfg.rafts[leader2].SayMeL())

	cfg.disconnect(other) // 下线另一个从节点

	//DPrintf(11, "%v: 1 follower is done now !!!！ and the raft is going to be added new items...\n", cfg.rafts[other].SayMeL())
	// lots more commands that won't commit
	for i := 0; i < 50; i++ {
		cfg.rafts[leader2].Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)
	// bring original leader back to life,
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}
	DPrintf(11, "all nodes are down....\n")

	cfg.connect((leader1 + 0) % servers)
	cfg.connect((leader1 + 1) % servers)
	cfg.connect(other)
	DPrintf(11, "3 are reconnected....\n")

	// lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}
	DPrintf(11, "check successfully!....\n")
	// now everyone
	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}

	cfg.one(rand.Int(), servers, true)

	DPrintf(11, "TestBackup2B finished!!!... ")
	cfg.end()
}

func TestCount2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B_10): RPC counts aren't too high")

	rpcs := func() (n int) {
		for j := 0; j < servers; j++ {
			n += cfg.rpcCount(j)
		}
		return
	}

	leader := cfg.checkOneLeader()

	total1 := rpcs()

	if total1 > 30 || total1 < 1 {
		t.Fatalf("too many or few RPCs (%v) to elect initial leader\n", total1)
	}

	var total2 int
	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader = cfg.checkOneLeader()
		total1 = rpcs()

		iters := 10
		starti, term, ok := cfg.rafts[leader].Start(1)
		if !ok {
			// leader moved on really quickly
			continue
		}
		cmds := []int{}
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31())
			cmds = append(cmds, x)
			index1, term1, ok := cfg.rafts[leader].Start(x)
			if term1 != term {
				// Term changed while starting
				continue loop
			}
			if !ok {
				// No longer the leader, so term has changed
				continue loop
			}
			if starti+i != index1 {
				t.Fatalf("Start() failed")
			}
		}

		for i := 1; i < iters+1; i++ {
			cmd := cfg.wait(starti+i, servers, term)
			if ix, ok := cmd.(int); ok == false || ix != cmds[i-1] {
				if ix == -1 {
					// term changed -- try again
					continue loop
				}
				t.Fatalf("wrong value %v committed for index %v; expected %v\n", cmd, starti+i, cmds)
			}
		}

		failed := false
		total2 = 0
		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); t != term {
				// term changed -- can't expect low RPC counts
				// need to keep going to update total2
				failed = true
			}
			total2 += cfg.rpcCount(j)
		}

		if failed {
			continue loop
		}

		if total2-total1 > (iters+1+3)*3 {
			t.Fatalf("too many RPCs (%v) for %v entries\n", total2-total1, iters)
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	time.Sleep(RaftElectionTimeout)

	total3 := 0
	for j := 0; j < servers; j++ {
		total3 += cfg.rpcCount(j)
	}

	if total3-total2 > 3*20 {
		t.Fatalf("too many RPCs (%v) for 1 second of idleness\n", total3-total2)
	}

	cfg.end()
}

func TestPersist12C(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): basic persistence")

	cfg.one(11, servers, true)
	DPrintf(111, "crash and re-start all nodes...")
	// crash and re-start all
	for i := 0; i < servers; i++ {
		cfg.start1(i, cfg.applier)
	}
	DPrintf(111, "attempting to reconnect every node...")
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
		cfg.connect(i)
	}

	cfg.one(12, servers, true)
	DPrintf(111, "check again...")
	leader1 := cfg.checkOneLeader()
	DPrintf(111, "check if leader exists...")

	cfg.disconnect(leader1)
	cfg.start1(leader1, cfg.applier)
	cfg.connect(leader1)

	cfg.one(13, servers, true)

	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)
	cfg.one(14, servers-1, true)
	cfg.start1(leader2, cfg.applier)
	cfg.connect(leader2)

	cfg.wait(4, servers, -1) // wait for leader2 to join before killing i3

	i3 := (cfg.checkOneLeader() + 1) % servers
	cfg.disconnect(i3)
	cfg.one(15, servers-1, true)
	cfg.start1(i3, cfg.applier)
	cfg.connect(i3)

	cfg.one(16, servers, true)

	cfg.end()
}

func TestPersist22C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): more persistence")

	index := 1
	for iters := 0; iters < 5; iters++ {
		cfg.one(10+index, servers, true)
		index++

		leader1 := cfg.checkOneLeader()

		cfg.disconnect((leader1 + 1) % servers)
		cfg.disconnect((leader1 + 2) % servers)

		cfg.one(10+index, servers-2, true)
		index++

		cfg.disconnect((leader1 + 0) % servers)
		cfg.disconnect((leader1 + 3) % servers)
		cfg.disconnect((leader1 + 4) % servers)

		cfg.start1((leader1+1)%servers, cfg.applier)
		cfg.start1((leader1+2)%servers, cfg.applier)
		cfg.connect((leader1 + 1) % servers)
		cfg.connect((leader1 + 2) % servers)

		time.Sleep(RaftElectionTimeout)

		cfg.start1((leader1+3)%servers, cfg.applier)
		cfg.connect((leader1 + 3) % servers)

		cfg.one(10+index, servers-2, true)
		index++

		cfg.connect((leader1 + 4) % servers)
		cfg.connect((leader1 + 0) % servers)
	}

	cfg.one(1000, servers, true)

	cfg.end()
}

func TestPersist32C(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): partitioned leader and one follower crash, leader restarts")

	cfg.one(101, 3, true)

	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 2) % servers)

	cfg.one(102, 2, true)

	cfg.crash1((leader + 0) % servers)
	cfg.crash1((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.start1((leader+0)%servers, cfg.applier)
	cfg.connect((leader + 0) % servers)

	cfg.one(103, 2, true)

	cfg.start1((leader+1)%servers, cfg.applier)
	cfg.connect((leader + 1) % servers)

	cfg.one(104, servers, true)

	cfg.end()
}

// Test the scenarios described in Figure 8 of the extended Raft paper. Each
// iteration asks a leader, if there is one, to insert a command in the Raft
// log.  If there is a leader, that leader will fail quickly with a high
// probability (perhaps without committing the command), or crash after a while
// with low probability (most likey committing the command).  If the number of
// alive servers isn't enough to form a majority, perhaps start a new server.
// The leader in a new term may try to finish replicating log entries that
// haven't been committed yet.
func TestFigure82C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): Figure 8")

	cfg.one(rand.Int(), 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		leader := -1
		for i := 0; i < servers; i++ {
			if cfg.rafts[i] != nil {
				_, _, ok := cfg.rafts[i].Start(rand.Int())
				if ok {
					leader = i
				}
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 {
			cfg.crash1(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if cfg.rafts[s] == nil {
				cfg.start1(s, cfg.applier)
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if cfg.rafts[i] == nil {
			cfg.start1(i, cfg.applier)
			cfg.connect(i)
		}
	}

	cfg.one(rand.Int(), servers, true)

	cfg.end()
}

func TestUnreliableAgree2C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, true, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): unreliable agreement")

	var wg sync.WaitGroup

	for iters := 1; iters < 50; iters++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iters, j int) {
				defer wg.Done()
				cfg.one((100*iters)+j, 1, true)
			}(iters, j)
		}
		cfg.one(iters, 1, true)
	}

	cfg.setunreliable(false)

	wg.Wait()

	cfg.one(100, servers, true)

	cfg.end()
}

func TestFigure8Unreliable2C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, true, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): Figure 8 (unreliable)")

	cfg.one(rand.Int()%10000, 1, true)

	nup := servers
	for iters := 0; iters < 200; iters++ {
		DPrintf(111, "this is the %d th iter...", iters)
		if iters == 200 {
			cfg.setlongreordering(true)
		}
		leader := -1
		for i := 0; i < servers; i++ {
			_, _, ok := cfg.rafts[i].Start(rand.Int() % 10000)
			if ok && cfg.connected[i] {
				leader = i
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 && (rand.Int()%1000) < int(RaftElectionTimeout/time.Millisecond)/2 {
			cfg.disconnect(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if cfg.connected[s] == false {
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if cfg.connected[i] == false {
			cfg.connect(i)
		}
	}

	cfg.one(rand.Int()%10000, servers, true)

	cfg.end()
}

func internalChurn(t *testing.T, unreliable bool) {

	servers := 5
	cfg := make_config(t, servers, unreliable, false)
	defer cfg.cleanup()

	if unreliable {
		cfg.begin("Test (2C): unreliable churn")
	} else {
		cfg.begin("Test (2C): churn")
	}

	stop := int32(0)

	// create concurrent clients
	cfn := func(me int, ch chan []int) {
		var ret []int
		ret = nil
		defer func() { ch <- ret }()
		values := []int{}
		for atomic.LoadInt32(&stop) == 0 {
			x := rand.Int()
			index := -1
			ok := false
			for i := 0; i < servers; i++ {
				// try them all, maybe one of them is a leader
				cfg.mu.Lock()
				rf := cfg.rafts[i]
				cfg.mu.Unlock()
				if rf != nil {
					index1, _, ok1 := rf.Start(x)
					if ok1 {
						ok = ok1
						index = index1
					}
				}
			}
			if ok {
				// maybe leader will commit our value, maybe not.
				// but don't wait forever.
				for _, to := range []int{10, 20, 50, 100, 200} {
					nd, cmd := cfg.nCommitted(index)
					if nd > 0 {
						if xx, ok := cmd.(int); ok {
							if xx == x {
								values = append(values, x)
							}
						} else {
							cfg.t.Fatalf("wrong command type")
						}
						break
					}
					time.Sleep(time.Duration(to) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(79+me*17) * time.Millisecond)
			}
		}
		ret = values
	}

	ncli := 3
	cha := []chan []int{}
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan []int))
		go cfn(i, cha[i])
	}

	for iters := 0; iters < 20; iters++ {
		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			cfg.disconnect(i)
		}

		if (rand.Int() % 1000) < 500 {
			i := rand.Int() % servers
			if cfg.rafts[i] == nil {
				cfg.start1(i, cfg.applier)
			}
			cfg.connect(i)
		}

		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			if cfg.rafts[i] != nil {
				cfg.crash1(i)
			}
		}

		// Make crash/restart infrequent enough that the peers can often
		// keep up, but not so infrequent that everything has settled
		// down from one change to the next. Pick a value smaller than
		// the election timeout, but not hugely smaller.
		time.Sleep((RaftElectionTimeout * 7) / 10)
	}

	time.Sleep(RaftElectionTimeout)
	cfg.setunreliable(false)
	for i := 0; i < servers; i++ {
		if cfg.rafts[i] == nil {
			cfg.start1(i, cfg.applier)
		}
		cfg.connect(i)
	}

	atomic.StoreInt32(&stop, 1)

	values := []int{}
	for i := 0; i < ncli; i++ {
		vv := <-cha[i]
		if vv == nil {
			t.Fatal("client failed")
		}
		values = append(values, vv...)
	}

	time.Sleep(RaftElectionTimeout)

	lastIndex := cfg.one(rand.Int(), servers, true)

	really := make([]int, lastIndex+1)
	for index := 1; index <= lastIndex; index++ {
		v := cfg.wait(index, servers, -1)
		if vi, ok := v.(int); ok {
			really = append(really, vi)
		} else {
			t.Fatalf("not an int")
		}
	}

	for _, v1 := range values {
		ok := false
		for _, v2 := range really {
			if v1 == v2 {
				ok = true
			}
		}
		if ok == false {
			cfg.t.Fatalf("didn't find a value")
		}
	}

	cfg.end()
}

func TestReliableChurn2C(t *testing.T) {
	internalChurn(t, false)
}

func TestUnreliableChurn2C(t *testing.T) {
	internalChurn(t, true)
}

const MAXLOGSIZE = 2000

func snapcommon(t *testing.T, name string, disconnect bool, reliable bool, crash bool) {
	iters := 20
	servers := 3
	cfg := make_config(t, servers, !reliable, true)
	defer cfg.cleanup()

	cfg.begin(name)

	cfg.one(rand.Int(), servers, true)
	leader1 := cfg.checkOneLeader()

	for i := 0; i < iters; i++ {
		DPrintf(1111, "this is the %d th iter", i)
		victim := (leader1 + 1) % servers
		sender := leader1
		if i%3 == 1 {
			sender = (leader1 + 1) % servers
			victim = leader1
		}

		if disconnect {
			cfg.disconnect(victim)
			cfg.one(rand.Int(), servers-1, true)
			DPrintf(111, "%v: 成功将节点断联并且日志仍被多数节点复制", cfg.rafts[victim].SayMeL())
		}
		if crash {
			cfg.crash1(victim)
			DPrintf(111, "%d 节点已经被下线", victim)
			cfg.one(rand.Int(), servers-1, true)
			DPrintf(111, "成功将节点%d下线", victim)
		}

		// perhaps send enough to get a snapshot
		nn := (SnapShotInterval / 2) + (rand.Int() % SnapShotInterval)
		DPrintf(111, "发送足够的日志以便后续校验其正确性")
		for i := 0; i < nn; i++ {
			cfg.rafts[sender].Start(rand.Int())
		}

		// let applier threads catch up with the Start()'s
		if disconnect == false && crash == false {
			// make sure all followers have caught up, so that
			// an InstallSnapshot RPC isn't required for
			// TestSnapshotBasic2D().
			DPrintf(111, "准备校验快照是否已发送给从节点....")
			cfg.one(rand.Int(), servers, true)
			DPrintf(111, "校验完毕....")

		} else {
			cfg.one(rand.Int(), servers-1, true)
			DPrintf(111, "快照或者日志已被大多数节点接收")
		}

		if cfg.LogSize() >= MAXLOGSIZE {
			cfg.t.Fatalf("Log size too large")
		}
		if disconnect {
			// reconnect a follower, who maybe behind and
			// needs to rceive a snapshot to catch up.
			cfg.connect(victim)
			cfg.one(rand.Int(), servers, true)
			leader1 = cfg.checkOneLeader()
		}
		if crash {
			cfg.start1(victim, cfg.applierSnap)
			DPrintf(111, "重新上线%d节点之前成功应用快照", victim)
			cfg.connect(victim)
			DPrintf(111, "%v: 节点上线成功！", cfg.rafts[victim].SayMeL())
			cfg.one(rand.Int(), servers, true)
			leader1 = cfg.checkOneLeader()
			DPrintf(111, "%v：节点%d重连后, 产生的leader为%d", cfg.rafts[leader1].SayMeL(), victim, leader1)
		}
	}
	cfg.end()
}

func TestSnapshotBasic2D(t *testing.T) {
	snapcommon(t, "Test (2D): snapshots basic", false, true, false)
}

func TestSnapshotInstall2D(t *testing.T) {
	snapcommon(t, "Test (2D): install snapshots (disconnect)", true, true, false)
}

func TestSnapshotInstallUnreliable2D(t *testing.T) {
	snapcommon(t, "Test (2D): install snapshots (disconnect+unreliable)",
		true, false, false)
}

func TestSnapshotInstallCrash2D(t *testing.T) {
	snapcommon(t, "Test (2D): install snapshots (crash)", false, true, true)
}

func TestSnapshotInstallUnCrash2D(t *testing.T) {
	snapcommon(t, "Test (2D): install snapshots (unreliable+crash)", false, false, true)
}

// do the servers persist the snapshots, and
// restart using snapshot along with the
// tail of the log?
func TestSnapshotAllCrash2D(t *testing.T) {
	servers := 3
	iters := 5
	cfg := make_config(t, servers, false, true)
	defer cfg.cleanup()

	cfg.begin("Test (2D): crash and restart all servers")

	cfg.one(rand.Int(), servers, true)

	for i := 0; i < iters; i++ {
		// perhaps enough to get a snapshot
		nn := (SnapShotInterval / 2) + (rand.Int() % SnapShotInterval)
		for i := 0; i < nn; i++ {
			cfg.one(rand.Int(), servers, true)
		}

		index1 := cfg.one(rand.Int(), servers, true)

		// crash all
		for i := 0; i < servers; i++ {
			cfg.crash1(i)
		}

		// revive all
		for i := 0; i < servers; i++ {
			cfg.start1(i, cfg.applierSnap)
			cfg.connect(i)
		}

		index2 := cfg.one(rand.Int(), servers, true)
		if index2 < index1+1 {
			t.Fatalf("index decreased from %v to %v", index1, index2)
		}
	}
	cfg.end()
}

// do servers correctly initialize their in-memory copy of the snapshot, making
// sure that future writes to persistent state don't lose state?
func TestSnapshotInit2D(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, true)
	defer cfg.cleanup()

	cfg.begin("Test (2D): snapshot initialization after crash")
	cfg.one(rand.Int(), servers, true)

	// enough ops to make a snapshot
	nn := SnapShotInterval + 1
	for i := 0; i < nn; i++ {
		cfg.one(rand.Int(), servers, true)
	}

	// crash all
	for i := 0; i < servers; i++ {
		cfg.crash1(i)
	}

	// revive all
	for i := 0; i < servers; i++ {
		cfg.start1(i, cfg.applierSnap)
		cfg.connect(i)
	}

	// a single op, to get something to be written back to persistent storage.
	cfg.one(rand.Int(), servers, true)

	// crash all
	for i := 0; i < servers; i++ {
		cfg.crash1(i)
	}

	// revive all
	for i := 0; i < servers; i++ {
		cfg.start1(i, cfg.applierSnap)
		cfg.connect(i)
	}

	// do another op to trigger potential bug
	cfg.one(rand.Int(), servers, true)
	cfg.end()
}
