package com.example.solaceconnector.dummy;


import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class DummySourceSplitSerializer implements SimpleVersionedSerializer<DummySourceSplit> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(DummySourceSplit split) throws IOException {
        return split.splitId().getBytes();
    }

    @Override
    public DummySourceSplit deserialize(int version, byte[] serialized) throws IOException {
        String splitId = new String(serialized);
        return new DummySourceSplit(splitId);
    }
}
