<h1 align="center">MIT6.5840（6.824）-Distributed-System Lab4B</h1>

The Lab4A's realization of MIT6.5840(also early called 6.824) Distributed System in Spring 2023

至于4A的实现，非常推荐大家看一下这篇csdn博客，
[MIT6.824-lab4A-2022（万字推导思路及代码构建）](https://blog.csdn.net/weixin_45938441/article/details/125386091?spm=1001.2014.3001.5502)

我的代码和这篇博客不相同，但是这个人讲解的非常好

这里我把分片控制器称之为配置中心，后面都是这样的。

# 1 数据库分片的基础知识
## 1.1 此lab中数据库的分片所采用的架构？

> 作业中讲到的：This lab's general architecture (a configuration service and a set of replica groups) 
> follows the same general pattern as Flat Datacenter Storage, BigTable, Spanner, FAWN, Ap
> ache HBase, Rosebud, Spinnaker, and many others. These systems differ in many details 
> from this lab, though, and are also typically more sophisticated and capable. For example, 
> the lab doesn't evolve the sets of peers in each Raft group; its data and query models are 
> very simple; and handoff of shards is slow and doesn't allow concurrent client access.

核心：a configuration service and a set of replica groups

Lab4A需要实现的就是其中的配置服务部分，也可以称为shard controller，其实现和Lab3A大同小异，**唯一比较耗时的
是负载均衡部分**。


## 1.2 讲一下”一个配置服务和一组复制组“这种架构的各个角色的作用

![img.png](images/img_33.png)

## 1.3 在“一个配置服务和一组复制组”架构下，读写请求如何打给复制组的？
![img_1.png](images/img_34.png)

在Lab3的introduction中也规定了我们必须得实现控制器的几个接口，
其中有一个Query接口，用于客户端查询相关键的配置信息，包括这个键值对存储在
哪一个复制组中，为什么需要定义这个接口呢，其目的就是给客户端提供具体键值对
所在复制组的信息以供其将读写请求打到具体的复制组

## 1.4 在“一个配置服务和一组复制组”架构下，客户端的读写请求需要两次访问
![img_2.png](images/img_35.png)

## 1.5 这种架构的其他应用

![img_3.png](images/img_36.png)


# 2 Lab4B任务分解

## 2.1 这里的一个复制组是怎么表示的？

## 2.2 server.go的ShardKV表示一个复制组的一个节点，怎么表示一个复制组

## 2.3 StartServer的方法参数说明

### 2.3.1 servers []*labrpc.ClientEnd, persister *raft.Persister, 

> 表示当前shardKV server所在的复制组的其他节点，这个一般用于传递给raft节点使用，因为底层的raft的leader节点需要
> 将日志和快照同步给从节点，就必须知道所有对等节点的信息

> persister同上
### 2.3.2 StartServer的方法参数gid有什么用？
> 方法注释上说用于和配置中心交互使用，但是具体会涉及到哪些操作呢？难道是会调用join，leave等方法将自己主动从配置中心
> 移除或者加入？

![img_1.png](images/img_1.png)

## 2.3.3 ctrlers []*labrpc.ClientEnd
这个参数表示可以向配置中心的所有节点发送消息的端口，通过这个可以调用配置中心的Query方法，从而获取最新的配置，
并且和本地获取到的配置进行比较，如果探测到发生了配置变更，则需要暂停向外提供key/value存储服务，先按照配置
将分片迁移完毕，然后再恢复服务，这一过程就是静态迁移，至于如何实现动态迁移，那是在challenge部分考虑的事情。

## 2.3.4 make_end func(string) *labrpc.ClientEnd
函数前的注释是这样说的：
> make_end(servername) turns a server name from a
> Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
> send RPCs. You'll need this to send RPCs to other groups.

意思就是会将从配置中心取得的服务器名（Config.Groups[gid][i]）转换成与目标复制组该服务器名对应的labrpc.ClientEnd，
这个ClientEnd相当于目标节点对应的端点，客户端只有取得这个端点才有资格访问目标节点的API

ctrlers []*labrpc.ClientEnd参数也是给特定机器发送rpc的，为什么与make_end的不同？

![img.png](img.png)

## 2.4 作为整个分布式KV系统的客户端，可以同时访问配置中心和复制组嘛？

> 答：是的，因为用户通过hash或者取模算法后知道自己存取的key所属的分片id后，需要往配置中心请求最新的配置，这个配置里面
> 包含了该分片id所属的复制组地址，然后客户端会再根据这个地址向复制组发送crud存取请求。

## 2.5 客户端是怎么知道配置中心发生了变化没有

![img_2.png](images/img_2.png)

![img_3.png](images/img_3.png)

我的方案：其实从common.go文件中的ErrWrongGroup就可以看出老师也推荐这种方案
![img_4.png](images/img_4.png)

## 2.6 当某一个复制组中的大多数节点下线时，如何处理？
> 这个复制组不能对外提供KV服务，但是不影响其他复制组的正常工作，也就是说存在一部分打到这个异常的复制组的请求
> 会被拒绝掉

## 2.7 如果各个复制组采用轮询的方式检查最新的配置有没有发生变化，只有leader才能轮询嘛
![img_5.png](images/img_5.png)

## 2.8 shardKV服务端怎么知道控制中心的配置是否发生变更，如果它每次获取的配置都是一样的呢
![img_6.png](images/img_6.png)

## 2.9 分片迁移相关的策略
1 当复制组决定迁移分片的时候，是不是得遍历数据库中的所有值，并且计算它们的key所属的分片id，再根据配置查询是否发生变动，如果是就需要迁移？
![img_7.png](images/img_7.png)

2 你说的分片索引策略，是指使用一个map，key为分片id，value为一个数据库中属于该分片的key值列表嘛
![img_8.png](images/img_8.png)

3 你给的map中，value是不是也可以是一个set集合，这样的话，增删改查的复杂度为O(1)
![img_9.png](images/img_9.png)

4 除了携带key，value以及操作类型外，客户端访问复制组时是否需要携带key所属的分片id？
> 可以携带，因为不影响客户端取得正确数据
> 
> 情况一：如果客户端使用的是最新的配置，其中某一个key的shard为1，
> 所属的复制组为B，但是所有的服务端使用的是旧配置， 这个key的shard为1（因为客户端服务端
> 都使用相同的key->shardId的映射规则），但是所属于的复制组为A，那么复制组B收到请求后
> 会拒绝服务，并且返回客户端一个ErrWrongGroup的错误，但是客户端会不断轮询这个复制组,
> 直到复制组A的轮询器从配置中心探测到了最新的配置并且迁移数据到B后，复制组B才会返回客户端
> 所需要的数据。
> 
> 情况二：如果客户端使用的是旧的配置，某一个key的shard为1，所属的复制组为B，服务端使用的新的配置。
> 所属的复制组为A，客户端会将请求打给复制组B但是会被响应一个ErrWrongGroup的错误，随后客户端
> 会访问配置中心拿到正确的配置后访问正确的复制组A，然后顺利取得数据

## 2.10 判断新旧配置时，为什么不推荐比较版本号，哈希值的方法
![img_10.png](images/img_10.png)

![img_11.png](images/img_11.png)
## 2.11 当一个复制组通过轮询知道了配置发生变更并且将一部分分片发送给目标复制组，目标复制组收到分片时的配置是旧的怎么办呢？
![img_12.png](images/img_12.png)

## 2.12 你给的代码中如何实现增量迁移
![img_13.png](images/img_13.png)

## 2.13 当一个复制组检测配置发生变更时，只想要将其不受自己管理的分片进行迁移，其他的不变，有什么好的实现方案吗?
![img_14.png](images/img_14.png)

## 2.14 当一个复制组检测到配置发生变更时，需要迁移完成后才能更新配置嘛
![img_15.png](images/img_15.png)

## 2.15 结合你给的关于"当一个复制组通过轮询知道了配置发生变更并且将一部分分片发送给目标复制组，目标复制组收到分片时的配置是旧的怎么办呢？"的答案，目标复制组收到一个版本号比自己大的迁移分片到自己的请求时也需要向配置中心拉取最新配置并且也需要发送切换到别的复制组，这有没有可能产生死锁呢
![img_16.png](images/img_16.png)

## 2.16 当源复制组向目标复制组发送迁移分片请求的时候，是不是也要去重？
![img_17.png](images/img_17.png)

## 2.17 leader节点发送分片数据给其他节点的过程应该不需要下放到raft节点对吧
![img_18.png](images/img_18.png)

## 2.18 在leader下放分片成功的结果到上层状态机执行切换配置成功的这段时间，客户端的请求打过来了呢？

> 粗粒度做法：这个阶段应该拒绝所有响应客户端的请求，保持重新分片到更新数据到更换配置这一整个过程的原子性

> 细粒度：使用一个map记录所有正在迁移中的分片，对于收到客户端的GET/PUT请求时查看该key对应的分片是否
> 存在这个map当中，如果不在就可以响应

## 2.19 如何保证保持重新分片（包括接收分片和发送分片全部成功）到更新数据到更换配置这一整个过程的原子性？

接收方回复来自发送方的过期的rpc，响应的状态字段怎么设置，发送方如何处理针对过期rpc的响应?
> 接收方响应的状态字段设置为ErrStaleGroup，发送方收到针对过期的rpc的回复时，可以视为接收方已经正确接收并且正确应用了发送的分片，
> 所以可以直接return掉。

这种现象出现的原因？
> 可以理解为网络拥堵，发送方重复发送了多个包含分片的rpc，然后接收方回复的多个rpc中可能状态为OK的正确响应也因为
网络拥堵迟到了，但是状态字段为ErrStaleGroup的可能早一些到达发送方，所以，发送方的处理如下：
```go
func (kv *ShardKV) sendShards2TargetGroup(op Op, data map[string]string, targetReplicaGroupNames []string) {

	kv.SeqId++
	args := MoveShardsArgs{Gid: kv.gid, SeqId: kv.SeqId, Data: data, Cfg: op.ConfigOp.Cfg, TranferringShards: op.ConfigOp.TranferringShards}

	for {
		// try each known server.
		for _, serverName := range targetReplicaGroupNames {
			var reply MoveShardsReply
			ok := kv.make_end(serverName).Call("ShardKV.receiveSharding", &args, &reply)
			// 直到Ok才返回
			if ok && reply.Err == OK {
				//fmt.Printf("向%d发送请求,结果返回成功", srv)
				return
			}
			if ok && reply.Err == ErrStaleConfig {
				//fmt.Printf("向%d发送请求,结果返回成功", srv)
				DPrintf(111, "接收方已经正确更新分片：ErrStaleConfig")
				return
			}

		}
		time.Sleep(100 * time.Millisecond)
	}
}
```

## 2.20 如果等所有分片迁移完成再下放raft是不是有这个问题：针对推的设置：分片迁移到一半突然宕机，那这个日志不就缺失了嘛

> 问题说明：意思是分片会迁移到复制组B，C，在迁移完B的后就宕机了，假设我们要求必须全部迁移完成才会下放
> "删除迁移完成的键值对"命令给raft，那么这里的键值对会删除不成功，同时有可能会产生应用配置的动作也应用不成功
> 重启后依然使用的是旧配置 但是一部分键已经迁移成功, 另一部分失败，这个时候如何处理呢？

答： 重启后状态机通过轮询协程会进一步检测到新配置，然后还会继续刚才的发送过程，因为没有记录已经成功发送给B，
所以B仍然会被发送分片但是会被拒收并且被B返回一个ErrOldConfig的状态，
但是A认为是成功接受了，所以就开始给C发送分片直到成功发送，这个时候就相当于能继续完成宕机前的配置更新操作了

## 2.21 复制组的加入和配置中心的配置切换过程

### 1 如果一个新的复制组加入系统，是先放置复制组还是还是先更新配置呢？

### 2 如果一个复制组要离开系统，是先更新配置还是先下线复制组？

### 3 刚开始各个复制组从配置中心的配置是初始化的空配置，此时配置一致，但是随后配置中心生成了一个新的配置后，各复制组如何处理？
![img_19.png](images/img_19.png)

### 4 复制组中的数据为空的时候，此时又探测到新配置，如何确定是否发送分片呢？

### 5 复制组生成一个新配置，这个配置中所有的分片都只分给一个复制组A，但是集群中有A和B两个复制组，难道这时候B复制组也要将所有的数据迁移到A嘛？
> 是的

### 6 复制组的初始配置中，所有的分片都分配给了gid为0的虚拟复制组，但是复制组A，B拉取到新配置的时候发现分片有一部分是分给自己的（难道要给自己发送rpc嘛），有一部分分给另一个复制组，这种情况如何处理？
> 这个时候可以针对corner case做特殊处理，比如初始时如果分片都为0，
> 初始时的各个复制组拥有的shard都是指向0的，这个阶段也不可能接收请求且数据库中数据量也为0，
> 所以可以做一个特判，当发现配置中存在指向全为0的shards数组时就可以下放给raft更新配置的日志，
> 但是在下放之前需要更新一下count为数组长度，这样才满足更新本地配置的条件。

### 7 如果复制组在配置中心都leave掉了，新配置中所有分片指向的gid变成了0，这个corner case如何处理?
这可能是表示发生了错误
![img_21.png](images/img_21.png)
### 8 有没有可能在配置中心更新初始配置（初始配置指分片的gid指向0）之前就有客户端的读写请求打到某一个复制组A呢？
> 不可能，因为客户端也需要先查询配置中心拿到具体的复制组gid，发现gid为0时则必然不可能打到编号非0的复制组

### 9 配置中心是否能保证给每个分片都规定一个管理它的复制组？
![img_20.png](images/img_20.png)

## 2.21 为什么这里的客户端的Get和Put请求在收到ErrWrongGroup,ErrWrongLeader错误时不自增seqId，而是复用原来的继续请求？
> 有两个好处：（1）复用原来的id可以避免seqId过快消耗完（2）从语义的角度看，seqId表示一次操作（put/append/get）而不是一个rpc请求，
> seqId+1表示前一个操作成功，以便于在日志和监控中跟踪操作
> 缺点：因为收到ErrWrongGroup的时候，客户端还会复用原来的seqId继续向新的目标复制组发送请求，所以这就要求
> seqMap（去重复请求的map，保存了客户端完成的最后一次操作的序列号）在移动分片的时候也需要移动给目标复制组！

下面这张图讲解了为什么http请求的requestId为什么一直递增，在哪种情况下可以复用
![img_22.png](images/img_22.png)
下面是问题涉及到的代码段
```go

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	//ck.mu.Lock()
	ck.seqId++
	args.ClientId = ck.clientId
	args.SeqId = ck.seqId
	//ck.mu.Unlock()

	for {
		shard := key2shard(key)
		//args.shardId = shard
		//ck.mu.Lock()

		gid := ck.config.Shards[shard]
		//ck.mu.Unlock()
		if servers, ok := ck.config.Groups[gid]; ok {
			//ck.mu.Unlock()
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey || reply.Err == ErrDuplicate) {
					//DPrintf(111, "成功PutAppend， reply.Err：%v", reply.Err)
					DPrintf(111, "从复制组%d中正确的put/append(%v)了shardId为%d的key, 其对应的value:%v", gid, args.Op, shard, args.Value)

					return
				} else if ok && reply.Err == ErrWrongGroup {
					break
				} else {
					//DPrintf(111, "err wrong leader")
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(130 * time.Millisecond)
		// ask controler for the latest configuration.
		//ck.mu.Lock()
		ck.config = ck.sm.Query(-1)
		//ck.mu.Unlock()
	}
}
```
举一个因为没有迁移seqMap而导致不一致的例子
![img_25.png](images/img_25.png)
## 2.22 迁移出去的旧数据不删除对请求有干扰嘛？为什么可以不用立即删除迁移完成的key，
> 迁移出去的旧数据即使不删除也不会对请求有干扰，因为配置应用成功后，请求被打到这个复制组会被拒绝

![img_23.png](images/img_23.png)

## 2.23 如果是按照切片发送rpc，则 如果给予切片成功，或者时间超时，这两种情况都需要进行GC掉不属于自己的切片， 为什么超时了也可以GC掉不属于自己的切片
![ig_24.png](images/img_24.png)
## 2.24 为什么要给每一个分片设置一个版本号？
> 答：因为分片分片数据的清理有滞后性，为了不让旧数据影响到分片迁移过程，需要设置一个版本号。
因为下面这段代码会用到，比如当一个分片里的数据起初在A，config版本号是1，然后被迁移到了复制组B，但是组A的这个分片
> 还没有被清理，随后客户端请求会要求全部delete掉B的该分片的数据，然后配置发生了变更，规定该分片的管理权授予了A，
> 然后又授予了C，然后A在遍历的时候会误以为属于该分片但是未清理的数据可以发送给C（因为不知道分片的版本）
```go
// gid == kv.gid：找出的是原来属于自己的分片
// cfg.Shards[shardId] != kv.gid ： 发生了不一致的分片
// kv.shardPersist[shardId].ConfigNum < kv.latestConfig.Num：分片里存储的配置号小于自己
if gid == kv.gid && cfg.Shards[shardId] != kv.gid && kv.shardPersist[shardId].ConfigNum < cfg.Num {
}
```
## 2.25 配置发生更改时 分片为空应该发送嘛？
![img_26.png](images/img_26.png)

## 2.26 配置不能跳着应用是嘛
![img_27.png](images/img_27.png)

## 2.27 发送分片时的细节

### 2.27.1 当一个复制组向另一个复制组发送分片时，如果目标复制组拥有的当前最新的配置号比发送复制组的小，就会返回一个"配置未到达"的错误，这是为什么呢？
> 

### 2.27.2 只有当一个复制组接收了所有属于自己的但是目前还在别的复制组的分片，发送了所有原来属于自己但是现在不属于自己管理的分片后，复制组才能切换配置，但是这难道不和2.27.1中相矛盾吗？
> 不矛盾，因为初始状态下，所有的复制组的配置就是最新的配置（大家的配置号都是0），此时就不会有2.27.1的"配置未到达"的错误，
> 所以这个时候大家都可以既发送分片，又接收分片，如果复制组A探测到了配置号为1的新配置就会尝试迁移分片到B，但是B没有探测到
> 就会返回一个"配置未到达"的错误，随后A重复发送分片，因为B采用轮询拉取配置，所以总是有可能使得A和B的配置号相等，
> 那么随后B就可以接收A的分片

### 2.27.3 如果发生配置变更时，分片全部发送且接收完毕，则此时所有的分片中的配置版本应该都是和新配置的版本号一样
（gpt4回答的不一定准确！）
![img_32.png](images/img_32.png)

## 2.28 如果配置变更频繁呢,在配置版本号为1时组A的分片9被put进了数据，然后在版本2中，这个分片9的数据需要迁往复制组B，此时迁移成功后，分片9的版本号是多少

## 2.29 如何实现阻塞式的配置变更，即在配置变更期间，所有复制组拒绝用户请求？
我刚开始只是设置了一个配置变更但是不知道还需要通知所有的复制组，所以就老是出现bug
![img_28.png](images/img_28.png)

## 2.30 为什么复制组处理客户端的GET/PUT请求需要做双重校验？
### 2.30.1 首先这里的双重校验是哪双重？
第一重校验发生在Server后端收到GET/PUT请求时就会校验携带的key所在分片是否由自己管辖，
以及这个分片是否已经到达;

第二重校验发生在状态机应用GET/PUT请求时会重复一遍第一重校验的动作

### 2.30.2 只有所有的待发送切片都成功被目标复制组接收，所有的待接收切片到达本地才能切换配置，为什么在Server后端收到GET/PUT请求时就会校验携带的key所在分片是否已经到达本地呢,但是我的配置都已经更换完成了，那不应该说明所有的分片都到达本地并且接受了吗
![img_29.png](images/img_29.png)

### 2.30.3 双重校验的作用
![img_30.png](images/img_30.png)

### 2.30.4 第一重校验是不是可以去掉？
![img_31.png](images/img_31.png)

## 3.1 Lab4a的具体任务？

> 答：从上面来看，我们的任务就是实现一个基于raft日志复制的配置服务中心，
> 跟Lab3非常的类似, 只不过4不需要实现快照功能，我们只需要将Lab3中的
> Put/Get/Append命令换成Query/Leave/Join/Move等命令即可。

## 3.2 一个gid代表一个复制组，这个复制组里的机器宕机后是不是可以加入别的复制组？
对，因为gid只要它不是正在被一个复制组使用中，那么它就可以重用

## 3.3 为什么这里不用对状态机进行持久化?
> 如果仅仅对下层的raft日志进行持久化，则无需持久化上层状态机的数据，因为可以通过重放日志恢复数据状态，
> 只有当需要对日志进行快照操作时才需要也对数据状态快照，并且持久化日志和状态，以便删除快照之前的日志，然后
> 再利用之后的日志和快照的数据状态进行快速重放。

## 3.4 结构体Config的num字段解析

![img_4.png](images/img_37.png)

![img_5.png](images/img_38.png)

## 3.5 所以一个集群中只有一个是配置是有效的对吗
![img_6.png](images/img_39.png)

## 3.6 当撤销掉/新增一个复制组后，如何对分片进行负载均衡？

### 3.6.1 当新增一个复制组时:

需要注意两点，

1 应该new 一个配置，做一个深拷贝，而不是直接将当前的最新配置的地址加到配置分片的末尾或浅拷贝

2 合并新旧复制组后，需要新建一个切片，将所有复制组的gid放入切片中，然后再排序，后面计算每个复制组需要
的分片数，以及重新分片的时候都需要按照gid切片的顺序处理，这是因为不同协程遍历同一个map的键时是乱序的，
所以很有可能导致分片状态在几个节点中不一致
 
```go

func (sc *ShardCtrler) RebalanceShardsForJoin(newGroups map[int][]string) {
	// 获取最新的配置
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    oldConfig.Num + 1,
		Shards: oldConfig.Shards,
		Groups: make(map[int][]string),
	}
	DPrintf(111, "join之前，检查到旧的复制组为:%v", oldConfig.Groups)

	// 合并旧的和新的复制组
	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	for gid, servers := range newGroups {
		newConfig.Groups[gid] = servers
	}

	// 计算目标分片数
	totalShards := len(oldConfig.Shards)
	totalGroups := len(newConfig.Groups)
	shardsPerGroup := totalShards / totalGroups
	extraShards := totalShards % totalGroups

	// 获取复制组ID并按顺序排序，因为
	// shardCounts := make(map[int]int)是一个map，在后面的遍历时for newGid, count := range shardCounts，这是乱序的，因为这个方法会在不同的状态机中执行，所以会导致分片结果不一致
	groupIDs := make([]int, 0, len(newConfig.Groups))
	for gid := range newConfig.Groups {
		groupIDs = append(groupIDs, gid)
	}
	sort.Ints(groupIDs)

	// 计算每个复制组需要的分片数量
	shardCounts := make(map[int]int)
	// 按顺序为每个GID分配分片
	for _, gid := range groupIDs {
		shardCounts[gid] = shardsPerGroup
		if extraShards > 0 {
			shardCounts[gid]++
			extraShards--
		}
	}

	// 重新分配分片
	// 大概思想是遍历每一个分片，如果该分片对应的gid所代表的复制组需要的分片为0，
	// 则表示这个分片可以分配给其他复制组，所以遍历所有复制组直到找到一个需要分片数
	// 大于0的组，然后就将这个分片给它
	for shard, gid := range newConfig.Shards {
		if shardCounts[gid] <= 0 {
			// 遍历已排序的复制组ID
			for _, newGid := range groupIDs {
				count := shardCounts[newGid]
				if count > 0 {
					newConfig.Shards[shard] = newGid
					shardCounts[newGid]--
					break
				}
			}
		} else {
			shardCounts[gid]--
		}
	}
	sc.configs = append(sc.configs, newConfig)

	DPrintf(1111, "[节点%d执行Join之后最新配置信息]: len(sc.configs)：%v, sc.configs[len(sc.configs)-1].Num： %v, sc.configs[len(sc.configs)-1].Shards： %v, sc.configs[len(sc.configs)-1].Groups： %v",
		sc.me, len(sc.configs), sc.configs[len(sc.configs)-1].Num, sc.configs[len(sc.configs)-1].Shards, sc.configs[len(sc.configs)-1].Groups)
}
```

### 3.6.2 当撤销一个复制组时的负载均衡怎么做到？

除了2.6.1中提到的两点，撤销一个复制组时还有一个额外需要处理的corner case，比如当增加一个gid为1复制组，
此时所有shards的都分配给了1，然后又撤销这个复制组时，此时shards应该都指向0，

```go
func (sc *ShardCtrler) RebalanceShardsForLeave(removedGIDs []int) {
	// 获取最新的配置
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    oldConfig.Num + 1,
		Shards: oldConfig.Shards,
		Groups: make(map[int][]string),
	}

	// 将 removedGIDs 转换为 map 以便快速查找
	removedGIDMap := make(map[int]bool)
	for _, gid := range removedGIDs {
		removedGIDMap[gid] = true
	}
	DPrintf(111, "待移除的复制组为：%v", removedGIDMap)

	// 合并旧的复制组，但不包括要移除的
	for gid, servers := range oldConfig.Groups {
		if !removedGIDMap[gid] {
			newConfig.Groups[gid] = servers
		}
	}
	DPrintf(111, "此时有效的复制组为: %v,长度为%d", newConfig.Groups, len(newConfig.Groups))
	// 移除后如果集群中复制组为0，则需要将所有分片的GID指定为0，意味着没有使用复制组
	// 同时在返回前将新配置加入组中
	if len(newConfig.Groups) == 0 {
		for shard, _ := range newConfig.Shards {
			newConfig.Shards[shard] = 0
		}
		// 将新配置添加到配置列表
		sc.configs = append(sc.configs, newConfig)
		return
	}
	// 计算目标分片数
	totalShards := len(oldConfig.Shards)
	totalGroups := len(newConfig.Groups)
	if totalGroups == 0 {
		// 不能有零个复制组
		return
	}
	shardsPerGroup := totalShards / totalGroups
	extraShards := totalShards % totalGroups

	// 获取复制组ID并按顺序排序
	groupIDs := make([]int, 0, len(newConfig.Groups))
	for gid := range newConfig.Groups {
		groupIDs = append(groupIDs, gid)
	}
	sort.Ints(groupIDs)

	// 计算每个复制组需要的分片数量
	shardCounts := make(map[int]int)
	// 按顺序为每个GID分配分片
	for _, gid := range groupIDs {
		shardCounts[gid] = shardsPerGroup
		if extraShards > 0 {
			shardCounts[gid]++
			extraShards--
		}
	}

	// 重新分配分片
	for shard, gid := range newConfig.Shards {
		if removedGIDMap[gid] || shardCounts[gid] <= 0 {
			// 遍历已排序的复制组ID
			for _, newGid := range groupIDs {
				count := shardCounts[newGid]
				if count > 0 {
					newConfig.Shards[shard] = newGid
					shardCounts[newGid]--
					break
				}
			}
		} else {
			shardCounts[gid]--
		}
	}
	// 将新配置添加到配置列表
	sc.configs = append(sc.configs, newConfig)
	DPrintf(1111, "[节点%d Leave后最新配置信息]: len(sc.configs)：%v, sc.configs[len(sc.configs)-1].Num： %v, sc.configs[len(sc.configs)-1].Shards： %v, sc.configs[len(sc.configs)-1].Groups： %v",
		sc.me, len(sc.configs), sc.configs[len(sc.configs)-1].Num, sc.configs[len(sc.configs)-1].Shards, sc.configs[len(sc.configs)-1].Groups)
}
```
