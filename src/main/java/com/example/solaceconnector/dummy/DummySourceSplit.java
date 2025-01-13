package com.example.solaceconnector.dummy;



import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

public class DummySourceSplit implements SourceSplit, Serializable {

    private final String splitId;

    public DummySourceSplit(String splitId) {
        this.splitId = splitId;
    }

    @Override
    public String splitId() {
        return splitId;
    }
}
