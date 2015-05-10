package com.nice;


import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
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
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import wd.RowKeyDistributorByHashPrefix;

import java.io.IOException;
import java.util.Properties;

import static org.kohsuke.args4j.ExampleMode.ALL;


/**
 * This class model the flow of the id-generation
 * the flow reads the session file from Sprint extracts the sessionID
 * As String and HBaseIDGen is invoked on each session to generate an ID if necessary
 */
public class OldApp {

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

    @Option(name="-filePath")
    private String filePath = "/";



    public static void main(String[] args) throws Throwable
    {
        new OldApp().doMain(args);
    }

    private void doMain(String[] args) throws IOException
    {
        CmdLineParser parser = new CmdLineParser(this);

        // if you have a wider console, you could increase the value;
        // here 80 is also the default
        parser.setUsageWidth(180);

        try {
            // parse the arguments.
            parser.parseArgument(args);

            // you can parse additional arguments if you want.
            // parser.parseArgument("more","args");

            // after parsing arguments, you should check
            // if enough arguments are given.
//            if( arguments.isEmpty() )
//                throw new CmdLineException(parser,"No argument is given");

        }
        catch( CmdLineException e )
        {
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.getMessage());
            System.err.println("java SampleMain [options...] arguments...");
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();

            // print option sample. This is useful some time
            System.err.println("  Example: java SampleMain"+parser.printExample(ALL));

            return;
        }

        System.out.println("input file: " + filePath);


        //create table if not exists.
        createHBaseTable();

        Configuration conf = new Configuration();
        conf.set("fs.default.name", HDFS);
        conf.set("mapred.job.tracker", JOB_TRACKER);
        conf.set("mapred.map.tasks", "84");

        JobConf jobConf = new JobConf(conf, OldApp.class);
        Properties properties = AppProps.appProps().setName("id-generator").setVersion("1.0").buildProperties(jobConf);

        FlowConnector flowConnector = new HadoopFlowConnector(properties);

        //create source and sink
        Tap sessionTap = new Hfs(new TextDelimited(false, "|"), filePath);
//        sessionTap.getScheme().setSourceFields( new Fields( "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10",
//                "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9", "b10",
//                "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10",
//                "d1", "d2", "d3", "d4", "d5" ) );
        sessionTap.getScheme().setSourceFields( new Fields( "a1" ) );
        Tap sink = new NullTap();


        Pipe idGeneratorPipe = new Pipe("id-generator");
        idGeneratorPipe = new Each(idGeneratorPipe, new HBaseIDGen());

        // connect the taps, pipes, etc., into a flow
        FlowDef flowDef = FlowDef.flowDef()
                .setName("id-generator")
                .addSource(idGeneratorPipe,sessionTap)
                .addTailSink(idGeneratorPipe, sink);

        Flow<JobConf> flow = flowConnector.connect(flowDef);
        flow.complete();    }

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
