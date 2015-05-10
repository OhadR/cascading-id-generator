package com.nice;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * This class generates ID
 */
public class HBaseIDGen extends BaseOperation implements Function {

    private HBaseDAL hBaseDAL;
    private HTable hTable;
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(HBaseIDGen.class);

    public HBaseIDGen() {
        hBaseDAL = new HBaseDAL();
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        try {
//            Connection connection = ConnectionFactory.createConnection(config);
//            Table table = connection.getTable(TableName.valueOf("table1"));

            hTable = new HTable(App.getConfiguration(), App.ID_GEN_TABLE_NAME);
            logger.info("@@@ Opened HTable:" + new String(hTable.getTableName()));
        } catch (IOException e) {
            logger.error("Cannot open HTable:" + e.getMessage());
        }
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry tupleEntry = functionCall.getArguments();
//NOTE: in "real data" we use this code, as 1st item is system-id and 2nd item is session-id
//        String systemIDAsString = tupleEntry.getTuple().getString(0);
//        String sessionIDAsString = tupleEntry.getTuple().getString(1);

//NOTE: in data from Vertica I use this line, as the file includes nothing but the session-id:
        String sessionIDAsString = tupleEntry.getTuple().getString( 0 );        //NOTE: new data
        logger.info( "@@@ systemID: "
//                + systemIDAsString
                + ", sessionID: " + sessionIDAsString);
        try {
            hBaseDAL.generateSessionID( sessionIDAsString,hTable );
        } catch (IOException e) {
            logger.error("Error generating ID - "+ e.getMessage());
        }
        functionCall.getOutputCollector().add(tupleEntry);
    }

    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
        try {
            hTable.close();
        } catch (IOException e) {
            logger.error("Cannot close HTable: "+ e.getMessage());
        }
    }
}
