<h1 align="center">MIT6.5840（6.824）-Distributed-System Lab4A</h1>

The Lab4A's realization of MIT6.5840(also early called 6.824) Distributed System in Spring 2023

至于4A的实现，非常推荐大家看一下这篇csdn博客，
[MIT6.824-lab4A-2022（万字推导思路及代码构建）](https://blog.csdn.net/weixin_45938441/article/details/125386091?spm=1001.2014.3001.5502)

我的代码和这篇博客不相同，但是这个人讲解的非常好

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

![img.png](images/img.png)

## 1.3 在“一个配置服务和一组复制组”架构下，读写请求如何打给复制组的？
![img_1.png](images/img_1.png)

在Lab3的introduction中也规定了我们必须得实现控制器的几个接口，
其中有一个Query接口，用于客户端查询相关键的配置信息，包括这个键值对存储在
哪一个复制组中，为什么需要定义这个接口呢，其目的就是给客户端提供具体键值对
所在复制组的信息以供其将读写请求打到具体的复制组

## 1.4 在“一个配置服务和一组复制组”架构下，客户端的读写请求需要两次访问
![img_2.png](images/img_2.png)

## 1.5 这种架构的其他应用

![img_3.png](images/img_3.png)

# 2 任务分解

## 2.1 Lab4a的具体任务？

> 答：从上面来看，我们的任务就是实现一个基于raft日志复制的配置服务中心，
> 跟Lab3非常的类似, 只不过4不需要实现快照功能，我们只需要将Lab3中的
> Put/Get/Append命令换成Query/Leave/Join/Move等命令即可。

## 2.2 一个gid代表一个复制组，这个复制组里的机器宕机后是不是可以加入别的复制组？


## 2.3 为什么这里不用对状态机进行持久化?
> 如果仅仅对下层的raft日志进行持久化，则无需持久化上层状态机的数据，因为可以通过重放日志恢复数据状态，
> 只有当需要对日志进行快照操作时才需要也对数据状态快照，并且持久化日志和状态，以便删除快照之前的日志，然后
> 再利用之后的日志和快照的数据状态进行快速重放。

## 2.4 结构体Config的num字段解析

![img_4.png](images/img_4.png)

![img_5.png](images/img_5.png)

## 2.5 所以一个集群中只有一个是配置是有效的对吗
![img_6.png](images/img_6.png)

## 2.6 当撤销掉/新增一个复制组后，如何对分片进行负载均衡？

### 2.6.1 当新增一个复制组时:

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

### 2.6.2 当撤销一个复制组时的负载均衡怎么做到？

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
