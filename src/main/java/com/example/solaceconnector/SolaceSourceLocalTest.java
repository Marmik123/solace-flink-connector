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

        String ddl = "CREATE TABLE T (" +
            "  before ROW(" +
            "    DAT_TXN TIMESTAMP(3)," +
            "    COD_CC_BRN_TXN INT," +
            "    COD_USERNO INT," +
            "    CTR_BATCH_NO INT," +
            "    REF_SYS_TR_AUD_NO BIGINT," +
            "    REF_SUB_SEQ_NO INT," +
            "    COD_ACCT_NO STRING," +
            "    TXT_TXN_DESC STRING," +
            "    DAT_VALUE TIMESTAMP(3)," +
            "    REF_CHQ_NO STRING," +
            "    COD_DRCR STRING," +
            "    COD_TXN_MNEMONIC INT," +
            "    COD_TXN_LITERAL STRING," +
            "    AMT_TXN DOUBLE," +
            "    FLG_PASBKUPD STRING," +
            "    RAT_CCY INT," +
            "    COD_MSG_TYP INT," +
            "    COD_PROC INT," +
            "    CTR_UPDAT_SRLNO INT," +
            "    DAT_POST TIMESTAMP(3)," +
            "    AMT_TXN_TCY DOUBLE," +
            "    COD_TXN_CCY INT," +
            "    RAT_CONV_TCLCY INT," +
            "    COD_SC INT," +
            "    COD_AUTH_ID STRING," +
            "    REF_CARD_NO STRING," +
            "    REF_TXN_NO STRING," +
            "    REF_USR_NO STRING," +
            "    TXT_ADDL_INFO STRING," +
            "    DAT_TS_TXN_INIT TIMESTAMP(3)," +
            "    DAT_TS_SENT_FOR_AUTH TIMESTAMP(3)," +
            "    DAT_TS_AUTH TIMESTAMP(3)," +
            "    DAT_TS_SUB_FOR_PROCESSING TIMESTAMP(3)," +
            "    DAT_TS_SENT_TO_HOST TIMESTAMP(3)," +
            "    DAT_TS_HOST_PROCESSING TIMESTAMP(3)," +
            "    COD_ENTITY_VPD INT" +
            "  )," +
            "  after ROW(" +
            "    DAT_TXN TIMESTAMP(3)," +
            "    COD_CC_BRN_TXN INT," +
            "    COD_USERNO INT," +
            "    CTR_BATCH_NO INT," +
            "    REF_SYS_TR_AUD_NO BIGINT," +
            "    REF_SUB_SEQ_NO INT," +
            "    COD_ACCT_NO STRING," +
            "    TXT_TXN_DESC STRING," +
            "    DAT_VALUE TIMESTAMP(3)," +
            "    REF_CHQ_NO STRING," +
            "    COD_DRCR STRING," +
            "    COD_TXN_MNEMONIC INT," +
            "    COD_TXN_LITERAL STRING," +
            "    AMT_TXN DOUBLE," +
            "    FLG_PASBKUPD STRING," +
            "    RAT_CCY INT," +
            "    COD_MSG_TYP INT," +
            "    COD_PROC INT," +
            "    CTR_UPDAT_SRLNO INT," +
            "    DAT_POST TIMESTAMP(3)," +
            "    AMT_TXN_TCY DOUBLE," +
            "    COD_TXN_CCY INT," +
            "    RAT_CONV_TCLCY INT," +
            "    COD_SC INT," +
            "    COD_AUTH_ID STRING," +
            "    REF_CARD_NO STRING," +
            "    REF_TXN_NO STRING," +
            "    REF_USR_NO STRING," +
            "    TXT_ADDL_INFO STRING," +
            "    DAT_TS_TXN_INIT TIMESTAMP(3)," +
            "    DAT_TS_SENT_FOR_AUTH TIMESTAMP(3)," +
            "    DAT_TS_AUTH TIMESTAMP(3)," +
            "    DAT_TS_SUB_FOR_PROCESSING TIMESTAMP(3)," +
            "    DAT_TS_SENT_TO_HOST TIMESTAMP(3)," +
            "    DAT_TS_HOST_PROCESSING TIMESTAMP(3)," +
            "    COD_ENTITY_VPD INT" +
            "  )" +
            ") WITH (" +
            "  'connector' = 'solace'," +
            "  'host' = 'https://10.226.183.137:55555'," +
            "  'vpn' = 'cdc'," +
            "  'username' = 'datalakeuser'," +
            "  'password' = 'hdfcbank123$'," +
            "  'queue' = 'q.hdfc.cdc.flexcube.flink.gm'" +
            ")";


        // Step 3: Register the Custom Source Connector - Mention solace connection details here 
        // tableEnv.executeSql(
        //     "CREATE TABLE T (" +
        //     "  subject STRING" +
            
        //     ") WITH (" +
        //     "  'connector' = 'solace'," +
        //     "  'host' = 'https://10.226.183.137:55555'," +
        //     "  'vpn' = 'cdc'," +
        //     "  'username' = 'datalakeuser'," +
        //     "  'password' = 'hdfcbank123$'," +
        //     "  'queue' = 'q.hdfc.cdc.flexcube.flink.gm'" +
        //     ")"
        // );

        tableEnv.executeSql(ddl);

        // Debug: Confirm the table creation
        tableEnv.executeSql("SHOW TABLES").print();
        // tableEnv.executeSql("SHOW CONNECTORS").print();

        // Step 4: Query Data from the Custom Source
        Table result = tableEnv.sqlQuery("SELECT * FROM T");

        // tableEnv.sqlQuery("SELECT * FROM T").execute().print();

        // Step 5: Convert Table to DataStream and Print
        tableEnv.toDataStream(result, Row.class).print();

        // Step 6: Execute the Flink Job    
        env.execute("Test Solace Connector");
    }
}
