package org.n0nb0at.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface RpcInterface extends VersionedProtocol {
    long versionID = 1L;

    String findName(long studentID);
}
