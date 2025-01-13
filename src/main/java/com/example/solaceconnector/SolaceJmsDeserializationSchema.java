// package com.example.solaceconnector;


// import java.io.IOException;
// import javax.jms.Message;
// import javax.jms.TextMessage;
// import org.apache.flink.api.common.serialization.DeserializationSchema;
// import org.apache.flink.api.common.typeinfo.TypeInformation;
// import org.apache.flink.configuration.ReadableConfig;
// import org.apache.flink.table.data.GenericRowData;
// import org.apache.flink.table.data.RowData;
// import org.apache.flink.table.data.StringData;

// public class SolaceJmsDeserializationSchema implements DeserializationSchema<RowData> {

//     private final ReadableConfig config;

//     public SolaceJmsDeserializationSchema(ReadableConfig config) {
//         this.config = config;
//     }
//     @Override
//     public RowData deserialize(byte[] message) throws IOException {
//         throw new UnsupportedOperationException(
//             "This method should not be called for JMS-based deserialization."
//         );
//     }

//     public RowData deserialize(Message message) throws Exception {
//         if (message instanceof TextMessage) {
//             String text = ((TextMessage) message).getText();
//             // Map text to RowData fields (example for a single column)
//             return GenericRowData.of(StringData.fromString(text));
//         }
//         throw new UnsupportedOperationException("Unsupported JMS message type: " + message.getClass());
//     }

//     @Override
//     public boolean isEndOfStream(RowData nextElement) {
//         return false;
//     }

//     @Override
//     public TypeInformation<RowData> getProducedType() {
//         return TypeInformation.of(RowData.class);
//     }
// }
