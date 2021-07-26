# Hadoop RPC 作业

### Code


RPC 接口 [RpcInterface](../../bigdata-exercise/src/main/java/org/n0nb0at/rpc/RpcInterface.java)

RPC 接口实现 [RpcInterfaceImpl](../../bigdata-exercise/src/main/java/org/n0nb0at/rpc/RpcInterfaceImpl.java)

RPC 服务端 [RpcServer](../../bigdata-exercise/src/main/java/org/n0nb0at/rpc/RpcServer.java)

RPC 客户端 [RpcClient](../../bigdata-exercise/src/main/java/org/n0nb0at/rpc/RpcClient.java)

数据存储查询接口
 [StoreInterface](../../bigdata-exercise/src/main/java/org/n0nb0at/rpc/StoreInterface.java)

数据存储查询内存实现
 [StoreInterface](../../bigdata-exercise/src/main/java/org/n0nb0at/rpc/StoreInterface.java)

### Result

服务端
![Server](Server.png)

客户端
![Client](Client.png)

内容比较简单，主要熟悉 `RPC` 调用流程。

工作使用 `gRPC`，学习过极客时间微课 [深入浅出gRPC](https://time.geekbang.org/column/article/4372)。其他有兴趣同学，可以配合老师的 RPC 实战项目相互印证的查看。