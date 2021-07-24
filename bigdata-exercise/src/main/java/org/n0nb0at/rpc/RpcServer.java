package org.n0nb0at.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class RpcServer {
    public static void main(String[] args) {
        RPC.Builder builder = new RPC.Builder(new Configuration());
        System.out.println("服务端 - 配置服务地址端口");
        // 配置地址端口
        builder.setBindAddress("127.0.0.1");
        builder.setPort(6565);

        System.out.println("服务端 - 配置协议接口");
        // 配置协议接口
        builder.setProtocol(RpcInterface.class);
        builder.setInstance(new RpcInterfaceImpl(new StoreMemoryImpl()));

        try {
            System.out.println("服务端 - 启动服务");
            RPC.Server server = builder.build();
            // 启动服务
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
