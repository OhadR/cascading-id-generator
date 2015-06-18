package com.nice;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.LoggerFactory;
import wd.RowKeyDistributorByHashPrefix;

import java.io.IOException;


/**
 * This class generates ID.
 * cascading Function (operate) and Operation (prepare)
 */
public class HBaseIDGen extends BaseOperation implements Function {

    private static String ID_GEN_TABLE_NAME = "GeneratedSessionID";      //NOTE: need to add tenant-name
    public static String ID_GEN_TABLE_NAME_CF = "d";
    public static String ID_GEN_TABLE_NAME_QF = "ID";
    static final int MAX_BUCKETS = 32;

    private HTable hTable;
    private boolean checkBeforePut;
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(HBaseIDGen.class);

    public HBaseIDGen() {}

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        JobConf config = (JobConf)flowProcess.getConfigCopy();
        String tenant = config.get("com.ohadr.tenant");
        checkBeforePut = Boolean.valueOf( config.get("com.ohadr.checkBeforePut") );

        try {
            hTable = new HTable( Config.instance().getConfiguration(config), HBaseDAL.getTableName( tenant ) );
            logger.info("@@@ Opened HTable:" + new String(hTable.getTableName()));
            logger.info("@@@ isCheckBeforePut:" + checkBeforePut);
        } catch (IOException e) {
            logger.error("Cannot open HTable:" + e.getMessage());
        }
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry tupleEntry = functionCall.getArguments();
        String systemIDAsString = tupleEntry.getTuple().getString(0);
        String sessionTypeAsString = tupleEntry.getTuple().getString(1);
        String sessionIDAsString = tupleEntry.getTuple().getString(2);

        logger.debug("@@@ systemID: " + systemIDAsString
                + " sessionType: " + sessionTypeAsString
                + ", sessionID: " + sessionIDAsString);
        try {
        	HBaseDAL.generateSessionID( systemIDAsString, sessionTypeAsString, sessionIDAsString, hTable, checkBeforePut );
            //counter:
        	flowProcess.increment("ID_MIGRATION", "sessions written to migration-table", 1);
        } catch (IOException e) {
            logger.error("Error generating ID - "
                            + "for systemID: " + systemIDAsString
                            + " sessionType: " + sessionTypeAsString
                            + ", sessionID: " + sessionIDAsString,
                    e.getMessage());
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
