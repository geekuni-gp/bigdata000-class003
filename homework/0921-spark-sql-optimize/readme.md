# SparkSQL 优化

## 题目一：如何避免小文件问题

### 小文件产生的问题

为了以后能完整的复习，我觉得还是要把小文件产生的问题也总结一下。老师上课讲到了两个小文件带来的问题：

1. 每个小文件的访问都会访问 NameNode，产生很多 rpc 请求，对 NameNode 造成网络压力
2. 读取数据时，数据源过多的小文件会产生过多的 task，中间 shuffle 也会很多，计算、网络资源都会存在压力

后续查询网上的一些资料，跟老师讲的差不多，稍有差异的大致是以下几点：

1. 除了获取文件数据时对 NameNode 的访问压力，还有小文件元信息数量问题，1 亿个文件元信息就会占用约 20G 存储空间（老师在之前 Hadoop 课程内也提到过）
2. 小文件分布通常是不连续的，对比大文件的顺序读，随机访读性能通常较差，且碎片化的存储有可能造成磁盘空间浪费

### 小文件的来源

1. 在计算过程中因分片分组导致的小文件
2. 源文件本身就是独立的小文件

### 避免方案

1. 避免产生：如因 partition reduce 等配置造成的小文件过多，可通过合理调整切分配置来减少
2. 对小文件进行合并，针对不同场景可以细分：
   - 老师课上讲到的，按时间序分桶的数据，可以在到达一个时间节点后（月初、年初），对历史数据进行一次合并
   - 按文件大小，配置合并阈值，定期检测符合合并大小区间的进行合并

## 题目二：实现Compact table command

与上次实现 `SHOW VERSION` 作业相类似，增加一个 SQL 语句及对应的 `Command` 类实现。

大致思路：

1、添加自定义语句到 `SqlBase.g4`：

``` SQL
| COMPACT TABLE target=tableIdentifier partitionSpec?
    (INTO fileNum=INTEGER_VALUE identifier)?                       #compactTable
```

2、添加 `visitCompactTable` 方法到 `SparkSqlParser.scala`

``` Scala
override def visitCompactTable(ctx: CompactTableContext): LogicalPlan = withOrigin(ctx) {
  CompactTableCommand()
}
```

3、实现 `CompactTableCommand` 具体合并逻辑

- [x] 存在 `ctx.partitionSpec`，则获取指定 partition 目录下的问题件进行操作：`Option(ctx.partitionSpec).map { ...}`
- [x] 通过 `ctx.fileNum` 执行合并后的文件数量
  
- [ ] 合并文件：？？？

***如何合并还没成功实现……基本思路是想批量读取后通过 `repartition` 算子进行操作，还在实验中。***

## 题目三：Insert命令自动合并小文件

