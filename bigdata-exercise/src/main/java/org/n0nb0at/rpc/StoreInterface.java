package org.n0nb0at.rpc;

public interface StoreInterface<T, Req> {
    T query(Req req);
}
