package com.nice;


import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.*;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import wd.RowKeyDistributorByHashPrefix;
import wd.RowKeyDistributorByOneBytePrefix;

import java.io.IOException;
import java.util.Properties;


/**
 * This class model the flow of the id-generation
 * the flow reads the session file from Sprint extracts the sessionID
 * As String and HBaseIDGen is invoked on each session to generate an ID if necessary
 */
public class App {

//    private static final String NAME_NODE_1 = "il1-r1-perf-nam-1.saas.workgroup";
    private static final String NAME_NODE_1 = "vbox.localdomain";
    private static final int MAX_BUCKETS = 32;
    public static String ID_GEN_TABLE_NAME = "ID_GEN";
    public static String ID_GEN_TABLE_NAME_CF = "d";
    public static String ID_GEN_TABLE_NAME_QF = "ID";
    //VBOX
//    private static final String HDFS = "hdfs://vbox.localdomain:9000";
//    private static final String JOB_TRACKER = "vbox.localdomain:9001";
    //CDU
	private static final String HDFS = "hdfs://" + NAME_NODE_1 + ":9000";
//	private static final String JOB_TRACKER = "il1-r1-perf-nam-2.saas.workgroup:9001";
    private static final String JOB_TRACKER = NAME_NODE_1+ ":9001";


    public static void main(String[] args) throws Throwable {

        //create table if not exists.
        createHBaseTable();

        String sessionFilePath = args[0];

        Configuration conf = new Configuration();
        conf.set("fs.default.name", HDFS);
        conf.set("mapred.job.tracker", JOB_TRACKER);
        conf.set("mapred.map.tasks", "84");

        JobConf jobConf = new JobConf(conf, App.class);
        Properties properties = AppProps.appProps().setName("id-generator").setVersion("1.0").buildProperties(jobConf);

        FlowConnector flowConnector = new HadoopFlowConnector(properties);

        //create source and sink
        Tap sessionTap = new Hfs(new TextDelimited(false, "|"), sessionFilePath);
        sessionTap.getScheme().setSourceFields( new Fields( "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10",
                "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9", "b10",
                "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10",
                "d1", "d2", "d3", "d4", "d5" ) );
        Tap sink = new NullTap();


        Pipe idGeneratorPipe = new Pipe("id-generator");
        idGeneratorPipe = new Each(idGeneratorPipe, new HBaseIDGen());

        // connect the taps, pipes, etc., into a flow
        FlowDef flowDef = FlowDef.flowDef()
                .setName("id-generator")
                .addSource(idGeneratorPipe,sessionTap)
                .addTailSink(idGeneratorPipe, sink);

        Flow<JobConf> flow = flowConnector.connect(flowDef);
        flow.complete();
    }

    public static void createHBaseTable() throws IOException {
        Configuration hBaseConfig = getConfiguration();
        HBaseAdmin hbaseAdmin = new HBaseAdmin(hBaseConfig);
        if (hbaseAdmin.tableExists(ID_GEN_TABLE_NAME) ) {
            return;
        }
        HTableDescriptor desc = new HTableDescriptor(ID_GEN_TABLE_NAME);
        desc.addFamily(new HColumnDescriptor(Bytes.toBytes(ID_GEN_TABLE_NAME_CF)));
        hbaseAdmin.createTable(desc, createSplitKeys( MAX_BUCKETS ));
    }


    /**
     * create split keys
     * @param numberOfBuckets
     * @return
     */
    private static byte[][] createSplitKeys(int numberOfBuckets) {
        RowKeyDistributorByHashPrefix distributor = new RowKeyDistributorByHashPrefix(new RowKeyDistributorByHashPrefix.OneByteSimpleHash(MAX_BUCKETS));
        byte[][] allDistributedKeys = distributor.getAllDistributedKeys(new byte[0]);
        return allDistributedKeys;
    }


    /**
     * This method sets the HBase configuration
     * @return - configuration
     */
    public static Configuration getConfiguration() {
        Configuration hBaseConfig = HBaseConfiguration.create();
        hBaseConfig.setInt("timeout", 120000);
        hBaseConfig.set("hbase.master", "*" + NAME_NODE_1 + ":9000*");
        hBaseConfig.set("hbase.zookeeper.quorum", NAME_NODE_1);
        hBaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
        return hBaseConfig;
    }

}
