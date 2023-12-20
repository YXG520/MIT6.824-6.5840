<h1 align="center">MIT6.5840（6.824）- 分布式系统（Distributed-System）课程Lab的复现</h1>

2023年春季MIT6.5840(之前也称为6.824)分布式系统的试验复现

这是MIT的6.824分布式系统课程的实验项目，主要使用Go语言进行开发。

# 1 项目结构

## 1.1 分支
项目共分为9个分支，每个分支精确对应课程中每个实验的部分，分支命名格式如Lab2A，Lab2B等。
每个分支都有对应的详细说明文档，**其中包括该分支的bug修复案例，知识点讲解，测试函数的测试点详解，以及一些常见问题解答
（面试可能会问到）**，
我相信通过这种循序渐进的分支方式，一定能帮到大家，因为我参考别人源码的时候，因为有的代码属于后面的
lab的内容，看的懵懵懂懂，借鉴了但是有很多bug。
仓库结构：

![img.png](images/img.png)
## 1.2 使用说明：
一般想要保证程序不出现bug，每一个part的测试次数至少1000+，我做到了1000次0bug，
然后使用每一个文件下的dstest脚本进行测试即可，里面有使用规则。
![img_4.png](images/img_4.png)

## 1.3 md文档分布
下面是各个分支的md文档局部截图：

Lab2A分支截图
![img_1.png](images/img_1.png)

Lab2B分支截图
![img_2.png](images/img_2.png)
...

Lab4B分支截图
![img_3.png](images/img_3.png)

## 1.4 相关资源
1. MIT 6.824 课程视频：[MIT6.824分布式系统](https://www.bilibili.com/video/BV1qk4y197bB/?spm_id_from=333.337.search-card.all.click)
2. 中文课程笔记链接：[gitBook-mit6.824分布式系统公开课笔记](https://mit-public-courses-cn-translatio.gitbook.io/mit6-824/)
3. 课程表：https://pdos.csail.mit.edu/6.824/
4. 相关 论文：
（1）[csdn raft论文翻译](https://blog.csdn.net/lengxiao1993/article/details/108524808)
（2）[Spanner: Google’s Globally-Distributed Database](https://pdos.csail.mit.edu/6.824/papers/spanner.pdf)
5. raft论文笔记（Notes）：https://thesquareplanet.com/blog/students-guide-to-raft/

这个项目只是对MIT 6.824课程的个人理解和实现，如果有任何问题或者建议，欢迎提出。

最后，祝愿每一位参与者在分布式系统的学习旅程中都有所收获！

## 1.5 耗时
基础：只有几年java开发经验和一些cpp的使用经验，从来没用过golang
总耗时：55天

Lab2A:10天

Lab2B:7天

Lab2C:1天

Lab2D:10天

Lab3A:5天

Lab3B:7天

Lab4A:5天

Lab4B:10天
# 2 声明
这个项目我是打算用作校招项目的，所以精耕细作，得益于GPT4的强大，很多不懂得地方通过gpt4给出的全面分析也被我弄透，
大家可以参考我的一篇关于raft面试题的总结博客：[口撕raft面试100问](https://blog.csdn.net/yxg520s/article/details/130977890?spm=1001.2014.3001.5502)

