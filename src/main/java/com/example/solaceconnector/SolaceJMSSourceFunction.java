package com.example.solaceconnector;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import com.example.solaceconnector.dummy.DummyJMSState;
import com.example.solaceconnector.dummy.DummyJMSStateSerializer;
import com.example.solaceconnector.dummy.DummySourceSplit;
import com.example.solaceconnector.dummy.DummySourceSplitSerializer;
import com.example.solaceconnector.dummy.DummySplitEnumerator;

public class SolaceJMSSourceFunction implements Source<RowData, DummySourceSplit, DummyJMSState> {

    private final String brokerUrl;
    private final String vpnName;
    private final String username;
    private final String password;
    private final String topic;
    private final String queue;

    public SolaceJMSSourceFunction(String brokerUrl, String vpnName, String username, String password, String topic, String queue) {
        this.brokerUrl = brokerUrl;
        this.vpnName = vpnName;
        this.username = username;
        this.password = password;
        this.topic = topic;
        this.queue=queue;
    }   

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED; // Streaming source
    }

    @Override
    public SourceReader createReader(SourceReaderContext readerContext) {
        return new SolaceSourceReader(brokerUrl, vpnName, username, password, topic, queue, readerContext);
    }

    @Override
    public SimpleVersionedSerializer<DummySourceSplit> getSplitSerializer() {
        return new DummySourceSplitSerializer(); // Provide a serializer for splits
    }

    @Override
    public SimpleVersionedSerializer<DummyJMSState> getEnumeratorCheckpointSerializer() {
        return new DummyJMSStateSerializer(); // Provide a serializer for enumerator state
    }

    @Override
    public SplitEnumerator<DummySourceSplit, DummyJMSState> createEnumerator(SplitEnumeratorContext<DummySourceSplit> enumContext) {
        return new DummySplitEnumerator(enumContext); // Create a new enumerator
    }

    @Override
    public SplitEnumerator<DummySourceSplit, DummyJMSState> restoreEnumerator(SplitEnumeratorContext<DummySourceSplit> enumContext, DummyJMSState checkpoint) {
        return new DummySplitEnumerator(enumContext, checkpoint); // Restore enumerator from checkpoint
    }
}
