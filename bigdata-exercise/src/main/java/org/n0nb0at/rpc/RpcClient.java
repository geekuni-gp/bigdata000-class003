package org.n0nb0at.rpc;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

public class RpcClient {
    public static final String host = "127.0.0.1";
    public static final int port = 6565;

    public static void main(String[] args) {
        try {
            List<String> reqStudentIDs = Arrays.asList(args);

            if (CollectionUtils.isEmpty(reqStudentIDs)) {
                System.out.println("客户端 - req is empty");
                System.exit(2);
            }

            for (String studentID : reqStudentIDs) {
                RpcInterface proxy = RPC.getProxy(RpcInterface.class, RpcInterface.versionID,
                        new InetSocketAddress(host, port), new Configuration());
                String result = proxy.findName(Long.parseLong(studentID));
                System.out.printf("客户端 - studentID:%s name is %s%n", studentID, result);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
