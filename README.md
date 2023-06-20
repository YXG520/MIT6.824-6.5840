<h1 align="center">MIT6.5840（6.824）-Distributed-System Lab3A</h1>

The Lab3B's realization of MIT6.5840(also early called 6.824) Distributed System in Spring 2023

# 1 快照的基础知识

## 1.1 什么是快照
![img_3.png](img_3.png)
## 1.2 快照的主要作用
![img_1.png](img_1.png)
## 1.3 raft的状态机进行快照时要保存哪些数据
![img.png](img.png)

## 1.4 宕机重启的节点如何依据快照恢复
![img_2.png](img_2.png)

## 1.5 现在我采取的快照策略是每当持久化日志的数据量达到一定大小时就会发送一次快照给raft，现在你能告诉我怎么做才能探测到数据量的变化吗
![img_4.png](img_4.png)

因为在raft模块实现持久化的时机包括raft节点的任期号，
投票字段以及日志以及每一次发生的变更，所以我们这里在
每一次leader状态机执行日志的时候都会判断一次
快照的大小是否达到了规定的大小，满足就进行一次快照，
这是最简单的方法