
<h1 align="center">MIT6.5840（6.824）-Distributed-System Lab2C</h1>

The Lab2C's realization of MIT6.5840(also early called 6.824) Distributed System in Spring 2023

# 1 关于raft持久化的一些Q&A

## Q1 为什么要进行持久化
> 持久化是 RAFT 算法的关键特性之一，用于确保数据在节点发生故障或系统重启等情况下不会丢失。
## Q2 RAFT 算法会持久化哪些东西

![img.png](images/img.png)

## Q3 提交的日志条目就已经被持久化了，那么未提交的日志条目在系统崩溃时会丢失吗？

![img_1.png](images/img_1.png)

## Q4 为什么要持久化一个节点的任期号？

![img_4.png](images/img_4.png)

问：这个图片中为什么节点A故障了后可能会不断以任期0产生I/O请求呢？
> 答：其实截图中的第二个方框也回答的很明白了，就是因为网络中rpc请求过多，网络拥堵，
> 此时新任leader的心跳可能无法到达故障后重连的节点A，所以A会经历好几波网络超时
> 时间，也就会以任期等于0的情况发起好几次leader选举的投票广播，这任期是一定比
> 其他节点都小的，所以是无效的投票请求，进一步加重了拥堵（其实即使持久化了也有可能
> 在网络拥堵情况下以任期号5发送很多网络请求，但是这些请求时合法，是有可能被其他节点
> 投票的，所以相比任期为0百分百不可能被投票的情况，这种不算浪费网络资源
> ），同时也浪费了大量的网络资源。

## Q5 为什么要持久化votedFor字段？

![img_2.png](images/img_2.png)

你能举个因为没有持久化votedFor字段而导致同一个任期出现两个leader的例子吗?
![img_3.png](images/img_3.png)


## Q6 持久化的时候需要保存commitIndex，lastApplied字段吗？
![img_7.png](images/img_7.png)
## Q6 何时开始持久化？
持久化时间遵循一个原则：何时会更新日志、任期以及votedFor字段时，何时
就要进行持久化

![img_5.png](images/img_5.png)

## Q7 为什么要持久化日志呢？
> 个人理解：首先能保证更快的达到数据一致性，也能节省网络带宽，
> 如果一个节点不持久化内存中100个日志项的信息，则宕机重启后它的信息为0，
> 需要主节点从第一个日志开始传递给从节点所有的日志信息，如果宕机前节点持久化了
> 100个日志项信息，那么重启后主节点只需要同步第101个及以后的日志项

## 8 leader可以直接把自己的所有日志传给崩溃后从节点，这样不持久化日志的一致性问题就能解决吧

![img_8.png](images/img_8.png)
 
# 2 分析现有代码框架
## 2.1 现在的persist方法

> save Raft's persistent state to stable storage, 
> where it can later be retrieved after a crash and restart.
> see paper's Figure 2 for a description of what should be persistent.
> 
> 大概这个方法是被自己编写的raft的文件调用的方法，调用后会进行持久化
> 操作

## 2.2 readPersist方法
> 网络断联重连后或者宕机重启后需要从持久化中的文件中恢复数据就会调用
此方法只会在创建raft实例的make方法中使用，因为这里的节点都是使用go
> 模拟的，对于网络节点先断连然后重连只能通过改变

> **本部分还有一个注意点，就是raft节点的日志要在读取持久化数据之前初始化，否则因为
> 没有给日志分配内存空间，读取到的都是空对象。**
> 在本part中体现在调用make方法时，顺序应该是这样的：
> rf.log = NewLog() rf.readPersist()，
> 而不是rf.log = NewLog() rf.readPersist()这样的顺序



# 3 从分析Lab2C测试点到弄懂整个Lab2C的coding框架
> 在debug的时候多分析一下测试程序的测试点是非常重要的，弄懂了测试结构就知道了
> 本lab的代码结构，因为这涉及到代码调用和实现，比如tester是如何让节点下线的，配置
> 是如何清除各个raft实例的，leader状态是如何被tester获取的，、
> 集群中各个节点的提交日志是如何被tester探测到的
## 3.1 Test (2B) - TestBasicAgree2B

### 1 允许重试的cfg.one(11, servers, true)方法
Q：首先弄清楚为什么这里允许重试呢？
```go
// do a complete agreement.
// it might choose the wrong leader initially,
// and have to re-submit after giving up.
// entirely gives up after about 10 seconds.
// indirectly checks that the servers agree on the
// same value, since nCommitted() checks this,
// as do the threads that read from applyCh.
// returns index.
// if retry==true, may submit the command multiple
// times, in case a leader fails just after Start().
// if retry==false, calls Start() only once, in order
// to simplify the early Lab 2B tests.
```

### 2 在tester主动重启主机后，如何做到读取持久化的数据的？

> 首先readPersist()只在make方法里使用，而make用来生成各个raft实例的，
> 我们可以看到这里有一个cfg.start1函数，我们点进去看可知里面有一个crash1方法，
> 该方法会调用ReadRaftState方法保存用户状态， 然后又重新调用make方法生成一个
> raft实例

```go

// start or re-start a Raft.
// if one already exists, "kill" it first.
// allocate new outgoing port file names, and a new
// state persister, to isolate previous instance of
// this server. since we cannot really kill it.
func (cfg *config) start1(i int, applier func(int, chan ApplyMsg)) {
	cfg.crash1(i)

	// a fresh set of outgoing ClientEnd names.
	// so that old crashed instance's ClientEnds can't send.
	cfg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[i][j] = randstring(20)
	}

	// a fresh set of ClientEnds.
	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		cfg.net.Connect(cfg.endnames[i][j], j)
	}

	cfg.mu.Lock()

	cfg.lastApplied[i] = 0

	// a fresh persister, so old instance doesn't overwrite
	// new instance's persisted state.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()

		snapshot := cfg.saved[i].ReadSnapshot()
		if snapshot != nil && len(snapshot) > 0 {
			// mimic KV server and process snapshot now.
			// ideally Raft should send it up on applyCh...
			err := cfg.ingestSnap(i, snapshot, -1)
			if err != "" {
				cfg.t.Fatal(err)
			}
		}
	} else {
		cfg.saved[i] = MakePersister()
	}

	cfg.mu.Unlock()

	applyCh := make(chan ApplyMsg)

	rf := Make(ends, i, cfg.saved[i], applyCh)

	cfg.mu.Lock()
	cfg.rafts[i] = rf
	cfg.mu.Unlock()

	go applier(i, applyCh)

	svc := labrpc.MakeService(rf)
	srv := labrpc.MakeServer()
	srv.AddService(svc)
	cfg.net.AddServer(i, srv)
}
```
# 3 实现

## 3.1 persist()与readPersist()如何确保缓冲区读写安全？
> bytes.Buffer 在这段代码中的使用并没有并发安全问题，因为
> 它只在单个 goroutine 中被使用，所以不存在并发访问的问题。
> 但是如果 bytes.Buffer 在多个 goroutine 中被共享并访问，
> 那么就需要添加并发控制来保证它的安全性。

## 3.2 在本raft工程中有几个地方需要使用持久化函数，请说明一下为什么？

### 3.2.1 在startElection方法中，当一个节点变成了candidate状态后

> 因为节点已经尝试了一次term自增的操作，也就说大家都会在不久之后自增，所以
> 即使重启后，大家的任期是一致的

## 3.3 defer rf.persist()中defer的作用

![img_6.png](images/img_6.png)

> defer有一个作用就是在一个协程内，当有多个defer语句时，发生了panic
> 异常时，先defer的语句后执行，在使用defer rf.persist()语句之前，
> 调用startElection之前会使用defer rf.mu.UnLock()解锁，
> 也就是说当发生panic异常，持久化工作会在解锁之前调用，这样就保证了
> 原子性，保证了持久化的安全性。

## 3.4 本project当中，有哪几个方法涉及到了持久化的操作？
> 答：
> 
> 1 StartElection中：当一个节点转化为Candidate节点的时候，意味着任期会自增
> 
> 2 RequestVote方法中：接收方收到Candidate节点发来请求投票的信息后，发现对方
> 任期比自己大的时候也会更新自己的任期，如果决定投票，votedFor字段也会发生改变
> 
> 3 AppendEntries方法中：当leader节点发送完AppendEntries RPC给从节点，其中
> 某一个从节点响应的term大于自身的term时候会更新任期并且转换为Follower。

## 3.5 当一个有持久化特征的raft节点崩溃后，重连后的任期是多少呢，是原任期还是持久化后的任期？

答：持久化后的任期。

## 3.6 宕机重启的机器如读取持久化数据后如何与其他的机器交互？

![img_9.png](images/img_9.png)
