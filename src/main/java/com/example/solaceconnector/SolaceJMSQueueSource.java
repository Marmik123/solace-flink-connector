package com.example.solaceconnector;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import java.io.ByteArrayInputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.jms.*;
import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;
import com.solacesystems.jms.message.SolTextMessage;
import com.example.solaceconnector.MessageProcessor;
// import io.netty.channel.ChannelOutboundBuffer.MessageProcessor;
import com.example.solaceconnector.message_builder.JMSHeadersBuilder;
import com.example.solaceconnector.message_builder.JMSPayloadBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class SolaceJMSQueueSource extends RichSourceFunction<RowData> {

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

    private static final Logger LOG = LoggerFactory.getLogger(SolaceJMSQueueSource.class);

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

        // Step 3: Create JMS Session.
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Step 4: Create JMS Topic and Consumer.
        Queue solaceQueue = session.createQueue(queue);
        consumer = session.createConsumer(solaceQueue);

        // 5.Receive Messages.
        while (isRunning) {
            try {
                // Fetch message from Solace
                Message message = consumer.receive(1000); // Replace with Solace fetch logic
                if (message != null) {
                    System.out.print("##############################################");
                    System.out.print(message.toString());
                    LOG.info("Message: {}", message);
                    System.out.print("##############################################");
                    String payload;
                    // MESSAGE ID
                    String messageID = message.getJMSMessageID();
                    // Topic Name
                    String topic = message.getJMSDestination().toString();
                    String GG_ID = message.getStringProperty("GG_ID");

                    //EXTRACT TIMESTAMP
                    // Long timestamp = message.getJMSTimestamp();
                    // SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    // String message_timestamp= sdf.format(new Date(timestamp));
                    // LOG.info("MESSAGE TIMESTAMP::::::"+message_timestamp);
                    // Building headers json from enumeration.
                    // String headersJson = JMSHeadersBuilder.buildHeadersJson(message);
                    // LOG.info("Headers JSON: ######", headersJson);
                    // LOG.info(" TOPIC: ####", topic);
                    // LOG.info("messageID $$$$", messageID);

                    //HEADERS EXTRACTION
                    Map<String, String> headers = new HashMap<>();
                    Enumeration<String> propertyNames = message.getPropertyNames();
            
                    // Iterate over all JMS Properties
                    while (propertyNames.hasMoreElements()) {
                        String propertyName = propertyNames.nextElement();
                        String propertyValue = message.getStringProperty(propertyName); // Fetch the property value
                        headers.put(propertyName, propertyValue); // Add to headers map
                    }
                    LOG.info("ONLY HEADERS MAP" +headers);
                    ObjectMapper objectMapper = new ObjectMapper();
                    String string_header= objectMapper.writeValueAsString(headers);
                    LOG.info("STRING HEADERS::::::"+string_header);
                    String solPayload = null;
                    
                    //PAYLOAD PARSING.
                    if (message instanceof TextMessage) {
                        solPayload = ((TextMessage) message).getText();
                    } else if (message instanceof BytesMessage) {
                        BytesMessage bytesMessage = (BytesMessage) message;
                        byte[] data = new byte[(int) bytesMessage.getBodyLength()];
                        bytesMessage.readBytes(data);
                        solPayload = new String(data); // Convert bytes to string
                    } else {
                        throw new IllegalArgumentException("Unsupported message type: " + message.getClass());
                    }
                    LOG.info("XMLSOL PAYLOAD::::::::"+solPayload);
                    XmlMapper xmlMapper=new XmlMapper();
                    xmlMapper.readTree(new ByteArrayInputStream(solPayload.getBytes()));
                    // Extract and parse SolPayload
                    // JsonNode solPayloadJson = JMSPayloadBuilder.extractAndParseSolPayload(message);

                    // Convert JsonNode to string for RowData
                    // String solPayloadString = solPayloadJson.toString();

                    // if (message instanceof SolTextMessage) {
                    // payload = ((SolTextMessage) message).getText(); // Extract text
                    // } else if (message instanceof TextMessage) {
                    // payload = ((TextMessage) message).getText(); // Handle TextMessage
                    // }
                    // // Create a map to store the entire message details
                    // Map<String, Object> messageData = new HashMap<>();
                    // MessageProcessor messageProcessor =new MessageProcessor();
                    // messageProcessor.processMessage(message)
                    // if (message != null) {
                    // String payload;

                    // // Check for different message types
                    // // if (message instanceof SolTextMessage) {
                    // // payload = ((SolTextMessage) message).getText(); // Extract text from
                    // SolTextMessage
                    // // } else if (message instanceof TextMessage) {
                    // // payload = ((TextMessage) message).getText(); // Handle standard
                    // TextMessage
                    // // } else {
                    // // // Handle other message types (e.g., BytesMessage, MapMessage)
                    // // throw new IllegalArgumentException("Unsupported message type: " +
                    // message.getClass());
                    // // }
                    // if (message instanceof SolTextMessage) {
                    // payload = ((SolTextMessage) message).getText(); // Extract text
                    // } else if (message instanceof TextMessage) {
                    // payload = ((TextMessage) message).getText(); // Handle TextMessage
                    // } else if (message instanceof BytesMessage) {
                    // BytesMessage bytesMessage = (BytesMessage) message;
                    // byte[] data = new byte[(int) bytesMessage.getBodyLength()];
                    // bytesMessage.readBytes(data);
                    // payload = new String(data); // Convert bytes to string
                    // } else if (message instanceof MapMessage) {
                    // MapMessage mapMessage = (MapMessage) message;
                    // Map<String, Object> mapData = new HashMap<>();
                    // Enumeration<String> keys = mapMessage.getMapNames();
                    // while (keys.hasMoreElements()) {
                    // String key = keys.nextElement();
                    // mapData.put(key, mapMessage.getObject(key));
                    // }
                    // payload = mapData.toString(); // Convert map to string
                    // } else {
                    // // Handle other types generically
                    // payload = message.toString();
                    // }
                    // // Convert message payload to byte[] for deserialization
                    // byte[] messageBytes = payload.getBytes();

                    // // Instantiate deserializer.
                    // DynamicMsgDeserializer deserializationSchema = new DynamicMsgDeserializer();

                    // // Use custom deserialization logic
                    // RowData rowData = deserializationSchema.deserialize(messageBytes);
                    // LOG.info("######ROWDATA######: {}", rowData);
                    // Emit the deserialized data
                    // synchronized (ctx.getCheckpointLock()) {
                    // ctx.collect(rowData);
                    // }
                    
                    // StringData.fromString(solPayloadString),
                    // StringData.fromString(headersJson),
                    
                    //  StringData.fromString(message_timestamp)
                    GenericRowData rowData = GenericRowData.of(
                            StringData.fromString(GG_ID),
                            StringData.fromString(messageID),
                            StringData.fromString(string_header)
                          );
                    LOG.info("####ROWDATA####" + rowData);
                    ctx.collect(rowData);
                } else {
                    LOG.info("MESSAGE IS NOT AVAILABLE");
                }
                // }
            } catch (Exception e) {
                // Log the exception and continue processing
                e.printStackTrace();
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        try {
            if (consumer != null)
                consumer.close();
            if (session != null)
                session.close();
            if (connection != null)
                connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
