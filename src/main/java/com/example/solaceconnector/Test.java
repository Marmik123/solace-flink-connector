package com.example.solaceconnector;



import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test {
    public static void main(String[] args) throws Exception {
        // Step 1: Initialize Flink's Execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Step 2: Configure Solace Source Function
        SolaceJMSQueueSource sourceFunction = new SolaceJMSQueueSource(
                "ws://localhost:8008", // Broker URL
                "default",               // VPN
                "default",                  // Username
                "admin",                  // Password
                "marmik"             // Topic
        );

        // Step 3: Add Source Function to Flink Job
        env.addSource(sourceFunction)
           .name("Solace JMS Source")
           .print(); // Print messages to the console for verification

        // Step 4: Execute the Flink Job
        env.execute("Solace JMS Source Test");
    }
}

