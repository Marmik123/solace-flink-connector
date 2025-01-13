package com.example.solaceconnector.dummy;



import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

public class DummySplitEnumerator implements SplitEnumerator<DummySourceSplit, DummyJMSState> {

    public DummySplitEnumerator(SplitEnumeratorContext<DummySourceSplit> context) {
    }

    public DummySplitEnumerator(SplitEnumeratorContext<DummySourceSplit> context, DummyJMSState checkpoint) {
    }

    @Override
    public void start() {
        // Assign a dummy split to each reader
        // context.assignSplits(Collections.singletonList(new DummySourceSplit("dummy-split")));
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        // Handle split request
    }

    @Override
    public void addSplitsBack(java.util.List<DummySourceSplit> splits, int subtaskId) {
        // Add splits back for reassignment
    }

    @Override
    public void addReader(int subtaskId) {
        // Handle new reader registration
    }

    @Override
    public DummyJMSState snapshotState(long checkpointId) throws Exception {
        return new DummyJMSState(); // Return dummy state
    }

    @Override
    public void close() {
        // Cleanup resources
    }
}
