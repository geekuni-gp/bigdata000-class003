package org.n0nb0at.rpc;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class RpcInterfaceImpl implements RpcInterface {

    private StoreInterface<String, Long> store = new StoreMemoryImpl();

    public RpcInterfaceImpl(StoreInterface store) {
        this.store = store;
    }

    @Override
    public String findName(long studentID) {
        System.out.printf("服务端 - 接收到请求 studentID: %d%n", studentID);
        String result = store.query(studentID);
        System.out.printf("服务端 - 响应结果: %s%n", result);
        return result;
    }

    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
        return RpcInterface.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return ProtocolSignature.getProtocolSignature(this, protocol, clientVersion, clientMethodsHash);
    }
}
