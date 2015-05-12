package com.nice;


import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import wd.RowKeyDistributorByHashPrefix;

import java.io.IOException;
import java.io.Serializable;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HBaseDAL implements Serializable
{
    private static final Logger logger = LoggerFactory.getLogger(HBaseDAL.class);

    /**
     * generates the ID and write to HBase
     * @param sessionIDAsString
     * @param hTable
     * @return true if already exists.
     * @throws IOException
     */
    public boolean generateSessionID(String sessionIDAsString,HTable hTable,
                                     boolean checkBeforePut) throws IOException
    {
        RowKeyDistributorByHashPrefix distributor =
                new RowKeyDistributorByHashPrefix(new RowKeyDistributorByHashPrefix.OneByteSimpleHash( App.MAX_BUCKETS) );

//        Put put = new Put(distributor.getDistributedKey(Bytes.toBytes(sessionIDAsString)));
        Put put = new Put(distributor.getDistributedKey( sessionIDAsString.getBytes() ));
        put.add(Bytes.toBytes(App.ID_GEN_TABLE_NAME_CF), Bytes.toBytes(App.ID_GEN_TABLE_NAME_QF),
                sessionIDAsString.getBytes() );

        boolean exists = false;
        if( checkBeforePut )
        {
            hTable.put( put );
        }
        else
        {
            exists = hTable.checkAndPut(distributor.getDistributedKey(Bytes.toBytes(sessionIDAsString)),
                    Bytes.toBytes(App.ID_GEN_TABLE_NAME_CF), Bytes.toBytes(App.ID_GEN_TABLE_NAME_QF), null, put);
        }

        return exists;
    }

}
