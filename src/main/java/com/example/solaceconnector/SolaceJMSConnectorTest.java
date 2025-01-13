// package com.example.solaceconnector;
// package com.example.solaceconnector;

// import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
// // import org.junit.jupiter.api.Test;
// // import static org.mockito.Mockito.*;
// // import static mockito.Mockito.*;
// import javax.jms.*;

// public class SolaceJMSConnectorTest {

//     // @Test
//     public void testMessageReception() throws Exception {
//         // Mock JMS Connection Factory
//         ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
//         Connection connection = mock(Connection.class);
//         Session session = mock(Session.class);
//         MessageConsumer consumer = mock(MessageConsumer.class);
//         TextMessage message = mock(TextMessage.class);

//         when(connectionFactory.createConnection()).thenReturn(connection);
//         when(connection.createSession(false, Session.AUTO_ACKNOWLEDGE)).thenReturn(session);
//         when(session.createTopic("test/topic")).thenReturn(mock(Topic.class));
//         when(session.createConsumer(any(Topic.class))).thenReturn(consumer);
//         when(consumer.receive(1000)).thenReturn(message);
//         when(message.getText()).thenReturn("Hello, Mock!");

//         // Simulate Source Function
//         SolaceJMSSourceFunction sourceFunction = new SolaceJMSSourceFunction(
//                 "ws://localhost:8008", // Replace with mock or actual broker URL
//                     "default",       // Replace with mock or actual VPN
//                     "default",      // Replace with mock or actual username
//                     "admin",  // Replace with mock or actual password
//                  "player/plays/cricket",
//                 "marmik"
//         );

//         // Run the Source Function
//         sourceFunction.run(new SourceContext<String>() {
//             @Override
//             public void collect(String element) {
//                 assert element.equals("Hello, Mock!");
//             }

//             @Override
//             public void collectWithTimestamp(String element, long timestamp) {}

//             // @Override
//             // public void emitWatermark(org.apache.flink.api.common.eventtime.Watermark mark) {}

//             @Override
//             public void markAsTemporarilyIdle() {}

//             @Override
//             public Object getCheckpointLock() {
//                 return this;
//             }

//             @Override
//             public void close() {}
//         });
//     }
// }
