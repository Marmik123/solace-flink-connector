package com.example.solaceconnector;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import javax.jms.*;
import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
public class SolaceJMSQueueSource extends RichSourceFunction<RowData>  {

    private final String brokerUrl;
    private final String vpnName;
    private final String username;
    private final String password;
    private final String queue;

    private volatile boolean isRunning = true;
    private transient Connection connection;
    private transient Session session;
    private transient MessageConsumer consumer;

    public SolaceJMSQueueSource(String brokerUrl, String vpnName, String username, String password, String queue) {
        this.brokerUrl = brokerUrl;
        this.vpnName = vpnName;
        this.username = username;
        this.password = password;
        this.queue = queue;
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        // Step 1: Create JMS Connection Factory
        SolConnectionFactory connectionFactory = SolJmsUtility.createConnectionFactory();
        connectionFactory.setHost(brokerUrl);
        connectionFactory.setVPN(vpnName);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);

        // Step 2: Create JMS Connection
        connection = connectionFactory.createConnection();
        connection.start();

        // Step 3: Create JMS Session
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Step 4: Create JMS Topic and Consumer
        Queue solaceQueue = session.createQueue(queue);
        consumer = session.createConsumer(solaceQueue);

        // Step 5: Receive Messages
        while (isRunning) {
            Message message = consumer.receive(1000); // Timeout of 1 second
            // if (message instanceof TextMessage) {
            //     String text = ((TextMessage) message).getText();
            //     // synchronized (ctx.getCheckpointLock()) {
            //     //     ctx.collect(text);
            //     // }
                
            //     ctx.collect(GenericRowData.of(StringData.fromString(text)));
            // }
            ctx.collect(GenericRowData.of(message));
        }   
    }

    @Override
    public void cancel() {
        isRunning = false;
        try {
            if (consumer != null) consumer.close();
            if (session != null) session.close();
            if (connection != null) connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
