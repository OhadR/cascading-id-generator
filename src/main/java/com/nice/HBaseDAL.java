package com.nice;


import com.ccih.common.wd.RowKeyDistributorByHashPrefix;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
//import wd.RowKeyDistributorByHashPrefix;

import java.io.IOException;
import java.io.Serializable;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HBaseDAL implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(HBaseDAL.class);
    private RowKeyDistributorByHashPrefix distributor;


    public HBaseDAL() {
        this.distributor = new RowKeyDistributorByHashPrefix(new RowKeyDistributorByHashPrefix.OneByteSimpleHash( App.MAX_BUCKETS) );
    }


    public long generateSessionID(String sessionIDAsString,HTable hTable) throws IOException {

        long milis = System.currentTimeMillis();

//        Put put = new Put(distributor.getDistributedKey(Bytes.toBytes(sessionIDAsString)));
        Put put = new Put(distributor.getDistributedKey( sessionIDAsString.getBytes() ));
        put.add(Bytes.toBytes(App.ID_GEN_TABLE_NAME_CF), Bytes.toBytes(App.ID_GEN_TABLE_NAME_QF),
//                Bytes.toBytes(milis));
                sessionIDAsString.getBytes() );

        boolean exists = hTable.checkAndPut(distributor.getDistributedKey(Bytes.toBytes(sessionIDAsString)),
                Bytes.toBytes(App.ID_GEN_TABLE_NAME_CF), Bytes.toBytes(App.ID_GEN_TABLE_NAME_QF), null, put);

        if (exists) {
            //get and return value
            Get get = new Get(distributor.getDistributedKey(sessionIDAsString.getBytes()));
            Result rs = hTable.get(get);
            byte[] id = rs.getValue(Bytes.toBytes(App.ID_GEN_TABLE_NAME_CF),
                    Bytes.toBytes(App.ID_GEN_TABLE_NAME_QF));
            return Bytes.toLong(id);
        }
        return milis;
    }

}
