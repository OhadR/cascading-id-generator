package com.nice;


import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import wd.RowKeyDistributorByHashPrefix;

import java.io.IOException;
import java.io.Serializable;



public class HBaseDAL implements Serializable
{
    /**
     * generates the ID and write to HBase
     * @param sessionIDAsString
     * @param hTable
     * @return true if already exists.
     * @throws IOException
     */
    public void generateSessionID(String sessionIDAsString,HTable hTable,
                                     boolean checkBeforePut) throws IOException
    {
        RowKeyDistributorByHashPrefix distributor =
                new RowKeyDistributorByHashPrefix( new OneByteMurmurHash(HBaseIDGen.MAX_BUCKETS) );

//        Put put = new Put(distributor.getDistributedKey(Bytes.toBytes(sessionIDAsString)));
        Put put = new Put(distributor.getDistributedKey( sessionIDAsString.getBytes() ));
        put.add(Bytes.toBytes(HBaseIDGen.ID_GEN_TABLE_NAME_CF), Bytes.toBytes(HBaseIDGen.ID_GEN_TABLE_NAME_QF),
                sessionIDAsString.getBytes() );

        if( checkBeforePut )
        {
            hTable.checkAndPut(distributor.getDistributedKey(Bytes.toBytes(sessionIDAsString)),
                    Bytes.toBytes(HBaseIDGen.ID_GEN_TABLE_NAME_CF), Bytes.toBytes(HBaseIDGen.ID_GEN_TABLE_NAME_QF), null, put);
        }
        else
        {
            hTable.put( put );
        }
    }

}
