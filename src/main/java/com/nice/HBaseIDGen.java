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
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
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
            createHBaseTable( config );

            hTable = new HTable( Config.instance().getConfiguration(config), getTableName( tenant ) );
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

    private void createHBaseTable(Configuration config) throws IOException
    {
        Configuration hBaseConfig = Config.instance().getConfiguration(config);

        String tenant = config.get("com.ohadr.tenant");

        HBaseAdmin hbaseAdmin = new HBaseAdmin(hBaseConfig);
        String tableName = getTableName( tenant );
        if (hbaseAdmin.tableExists( tableName ) ) {
            logger.info("*** NOTE: Table " + tableName + " already exists.");
            return;
        }
        HTableDescriptor desc = new HTableDescriptor( tableName );
        desc.addFamily(new HColumnDescriptor(Bytes.toBytes(ID_GEN_TABLE_NAME_CF)));
        hbaseAdmin.createTable(desc, createSplitKeys());

        logger.info("created table name: " + tableName );
    }

    /**
     * create split keys
     * @return
     */
    private static byte[][] createSplitKeys() {
        RowKeyDistributorByHashPrefix distributor = new RowKeyDistributorByHashPrefix(new OneByteMurmurHash(MAX_BUCKETS));
        byte[][] allDistributedKeys = distributor.getAllDistributedKeys(new byte[0]);
        return allDistributedKeys;
    }

    private static String getTableName(String tenant)
    {
        return tenant + ID_GEN_TABLE_NAME;
    }



}
