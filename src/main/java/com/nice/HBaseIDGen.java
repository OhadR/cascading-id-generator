package com.nice;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * This class generates ID.
 * cascading Function (operate) and Operation (prepare)
 */
public class HBaseIDGen extends BaseOperation implements Function {

    private HBaseDAL hBaseDAL;
    private HTable hTable;
    private boolean checkBeforePut;
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(HBaseIDGen.class);

    public HBaseIDGen() {
        hBaseDAL = new HBaseDAL();
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        JobConf config = (JobConf)flowProcess.getConfigCopy();
        String tenant = config.get("com.ohadr.tenant");
        checkBeforePut = Boolean.valueOf( config.get("com.ohadr.checkBeforePut") );
        try {
//            Connection connection = ConnectionFactory.createConnection(config);
//            Table table = connection.getTable(TableName.valueOf("table1"));

            hTable = new HTable( Config.instance().getConfiguration(), App.getTableName( tenant ) );
            logger.info("@@@ Opened HTable:" + new String(hTable.getTableName()));
            logger.info("@@@ isCheckBeforePut:" + checkBeforePut);
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
        logger.debug("@@@ systemID: "
//                + systemIDAsString
                + ", sessionID: " + sessionIDAsString);
        try {
            hBaseDAL.generateSessionID( sessionIDAsString, hTable, checkBeforePut );
        } catch (IOException e) {
            logger.error("Error generating ID - "+ e.getMessage());
        }
        functionCall.getOutputCollector().add(tupleEntry);
    }

    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
        try {
            logger.info("@@@ closing table..." );
            hTable.close();
        } catch (IOException e) {
            logger.error("Cannot close HTable: "+ e.getMessage());
        }
    }
}
