package com.example.solaceconnector;


import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import javax.jms.*;
import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

public class SolaceSourceReader implements SourceReader<RowData, SourceSplit> {

    private final BlockingQueue<RowData> messageQueue = new ArrayBlockingQueue<>(1000);
    private volatile boolean running = true;

    private final String brokerUrl;
    private final String vpnName;
    private final String username;
    private final String password;
    private final String topic;
    private final String queue;

    public SolaceSourceReader(String brokerUrl, String vpnName, String username, String password, String topic,String queue, SourceReaderContext context) {
        this.brokerUrl = brokerUrl;
        this.vpnName = vpnName;
        this.username = username;
        this.password = password;
        this.topic = topic;
        this.queue=queue;

        // Start a thread to consume messages and push them to the queue
        new Thread(this::consumeMessages).start();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws InterruptedException {
        RowData message = messageQueue.poll();
        if (message != null) {
            output.collect(message);
            return InputStatus.MORE_AVAILABLE;
        }
        return running ? InputStatus.NOTHING_AVAILABLE : InputStatus.END_OF_INPUT;
    }

    @Override
    public void close() throws Exception {
        running = false;
    }

    private void consumeMessages() {
        try {
            // Step 1: Create JMS Connection Factory
            SolConnectionFactory connectionFactory = SolJmsUtility.createConnectionFactory();
            connectionFactory.setHost(brokerUrl);
            connectionFactory.setVPN(vpnName);
            connectionFactory.setUsername(username);
            connectionFactory.setPassword(password);

            // Step 2: Create JMS Connection
            Connection connection = connectionFactory.createConnection();
            connection.start();

            // Step 3: Create JMS Session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Step 4: Create JMS Topic and Consumer
            Topic solaceTopic = session.createTopic(topic);
            MessageConsumer consumer = session.createConsumer(solaceTopic);

            // Step 5: Receive Messages
            while (running) {
                Message message = consumer.receive(1000);
                if (message instanceof TextMessage) {
                    String text = ((TextMessage) message).getText();
                    System.out.println(text);
                    messageQueue.offer(GenericRowData.of(
                        // StringData.fromString(text),
                        StringData.fromString("Subject 1")
                        ));
                }
            }
            //TESTING WITH MOCK DATA
           
            consumer.close();
            session.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void addSplits(List<SourceSplit> arg0) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addSplits'");
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isAvailable'");
    }

    @Override
    public void notifyNoMoreSplits() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'notifyNoMoreSplits'");
    }

    @Override
    public List<SourceSplit> snapshotState(long arg0) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'snapshotState'");
    }

    @Override
    public void start() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'start'");
    }

    
}
