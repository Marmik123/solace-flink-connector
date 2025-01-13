package com.example.solaceconnector.dummy;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class DummyJMSStateSerializer implements SimpleVersionedSerializer<DummyJMSState> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(DummyJMSState state) throws IOException {
        return new byte[0]; // No-op for simplicity
    }

    @Override
    public DummyJMSState deserialize(int version, byte[] serialized) throws IOException {
        return new DummyJMSState(); // No-op for simplicity
    }
}
