package com.example.solaceconnector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SolaceSourceLocalTest {

    public static void main(String[] args) throws Exception {
        // Step 1: Initialize the Flink Execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2); // Adjust parallelism for local testing

        // Step 2: Initialize the Flink Table Environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Step 3: Register the Custom Source Connector
        tableEnv.executeSql(
            "CREATE TABLE T (" +
            "  subject STRING" +
            
            ") WITH (" +
            "  'connector' = 'solace-jms'," +
            "  'host' = 'ws://localhost:8008'," +
            "  'vpn' = 'default'," +
            "  'user' = 'admin'," +
            "  'password' = 'admin'," +
            "  'queue' = 'marmik'" +
            ")"
        );

        // Debug: Confirm the table creation
        tableEnv.executeSql("SHOW TABLES").print();
        // tableEnv.executeSql("SHOW CONNECTORS").print();

        // Step 4: Query Data from the Custom Source
        Table result = tableEnv.sqlQuery("SELECT * FROM T");

        // Step 5: Convert Table to DataStream and Print
        tableEnv.toDataStream(result, Row.class).print();

        // Step 6: Execute the Flink Job
        env.execute("Test Solace Connector");
    }
}
