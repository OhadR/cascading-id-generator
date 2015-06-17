package com.nice;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.slf4j.LoggerFactory;
import wd.RowKeyDistributorByHashPrefix;

import java.io.IOException;
import java.io.Serializable;



public class HBaseDAL implements Serializable
{
    private static final int MAX_BUCKETS = 32;
    private static String ID_GEN_TABLE_NAME = "GeneratedSessionID";      //NOTE: need to add tenant-name
    private static String ID_GEN_TABLE_NAME_CF = "d";
    public static String ID_GEN_TABLE_NAME_QF = "GeneratedID";
    public static final String DELIMITER = "_";

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(HBaseDAL.class);

    /**
     * Create a new table in HBase with a limited number of versions
     * "new version", to support latest changes.
     *
     */
    public static void createHBaseTable(Configuration config) throws IOException, ZooKeeperConnectionException, MasterNotRunningException {

        Configuration hBaseConfig = Config.instance().getConfiguration(config);
        String tenant = config.get( Config.CONF__TENANT );
        String tenantAwareTableName = getTableName(tenant);

        HTableDescriptor hTableDescriptor = new HTableDescriptor(tenantAwareTableName);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor( ID_GEN_TABLE_NAME_CF );

        // Default - do not save data versions
        hColumnDescriptor.setMaxVersions(1);

        // set compression algorithm if not null
        hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);

        // set bloom filter if not null
        hColumnDescriptor.setBloomFilterType(StoreFile.BloomType.ROW);

        hTableDescriptor.addFamily(hColumnDescriptor);

        HBaseAdmin hbaseAdmin = new HBaseAdmin(hBaseConfig);

        if (hbaseAdmin.tableExists( tenantAwareTableName ) ) {
            logger.info("*** NOTE: Table " + tenantAwareTableName + " already exists.");
            return;
        }
        try {
            hbaseAdmin.createTable( hTableDescriptor, createSplitKeys( hBaseConfig) );
            logger.info("created table name: " + tenantAwareTableName);
        }
        finally {
            hbaseAdmin.close();
        }

    }

    public static String getTableName(String tenant)
    {
        return tenant + ID_GEN_TABLE_NAME;
    }

    /**
     * create split keys
     * @return
     */
    private static byte[][] createSplitKeys(Configuration hbaseConfig) throws IOException {
        RegionSplitter.SplitAlgorithm splitAlgorithm = RegionSplitter.newSplitAlgoInstance(hbaseConfig, "UniformSplit");
        splitAlgorithm.setLastRow(Bytes.toStringBinary(new byte[]{(byte)(MAX_BUCKETS)}));
        return splitAlgorithm.split(MAX_BUCKETS);
    }
    
    /**
     * generates the ID and write to HBase
     * @param sessionIDAsString
     * @param hTable
     * @return true if already exists.
     * @throws IOException
     */
    public static void generateSessionID(String systemIDAsString,
                                  String sessionTypeAsString,
                                  String sessionIDAsString,
                                  HTable hTable,
                                  boolean checkBeforePut) throws IOException
    {
        String idStr = systemIDAsString + DELIMITER + sessionTypeAsString + DELIMITER + sessionIDAsString;

        RowKeyDistributorByHashPrefix distributor =
                new RowKeyDistributorByHashPrefix( new OneByteMurmurHash(HBaseIDGen.MAX_BUCKETS) );

        byte[] distributedKey = distributor.getDistributedKey(idStr.getBytes());
        Put put = new Put( distributedKey );
        put.add(Bytes.toBytes(HBaseIDGen.ID_GEN_TABLE_NAME_CF), Bytes.toBytes(HBaseIDGen.ID_GEN_TABLE_NAME_QF),
                sessionIDAsString.getBytes() );

        if( checkBeforePut )
        {
            hTable.checkAndPut( distributedKey,
                    Bytes.toBytes(HBaseIDGen.ID_GEN_TABLE_NAME_CF), Bytes.toBytes(HBaseIDGen.ID_GEN_TABLE_NAME_QF), null, put);
        }
        else
        {
            hTable.put( put );
        }
    }

}
