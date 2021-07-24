package org.n0nb0at.rpc;

import java.util.HashMap;
import java.util.Map;

public class StoreMemoryImpl implements StoreInterface<String, Long> {

    public static final Map<Long, String> memoryStore =  new HashMap<Long, String>() {{
        put(20210123456789L, "心心");
        put(20190343020028L, "N0nb0at");
    }};

    @Override
    public String query(Long studentID) {
        return memoryStore.get(studentID);
    }
}
