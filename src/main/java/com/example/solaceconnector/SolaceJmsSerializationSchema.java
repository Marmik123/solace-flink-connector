// package com.example.solaceconnector;



// import java.nio.charset.StandardCharsets;

// import org.apache.flink.api.common.serialization.SerializationSchema;
// import org.apache.flink.table.data.RowData;

// public class SolaceJmsSerializationSchema implements SerializationSchema<RowData> {

//     @Override
//     public byte[] serialize(RowData element) {
//         return element.toString().getBytes(StandardCharsets.UTF_8);
//     }
// }
