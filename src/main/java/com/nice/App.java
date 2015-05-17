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
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.Properties;

import static org.kohsuke.args4j.ExampleMode.ALL;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;


/**
 * This class model the flow of the id-generation
 * the flow reads the session file from Sprint extracts the sessionID
 * As String and HBaseIDGen is invoked on each session to generate an ID if necessary
 */
public class App {

    @Option(name="-filePath")
    private String filePath = "/";

    @Option(name="-nameNode")
    private String nameNode = "vbox.localdomain";

    @Option(name="-zookeeper")
    private String zookeeper = "vbox.localdomain";      //bda: isr-r0-bda-dat-1.lab.il.nice.com

    @Option(name="-zookeeperPort")
    private String zookeeperPort = "2181";

    @Option(name="-checkBeforePut")
    private Boolean checkBeforePut = false;

    @Option(name="-tenant")
    private String tenant;

    public static void main(String[] args) throws Throwable
    {
        new App().doMain(args);
    }

    private void doMain(String[] args) throws IOException {
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
        System.out.println("nameNode: " + nameNode);
        System.out.println("zookeeper: " + zookeeper);
        System.out.println("zookeeperPort: " + zookeeperPort);
        System.out.println("checkBeforePut? " + checkBeforePut);

        String HDFS = "hdfs://" + nameNode + ":9000";
        String JOB_TRACKER = nameNode + ":9001";

        Configuration conf = new Configuration();
        conf.set("fs.default.name", HDFS);
        conf.set("mapred.job.tracker", JOB_TRACKER);
        conf.set("mapred.map.tasks", "84");
        conf.set("com.ohadr.tenant", tenant);
        conf.set("com.ohadr.checkBeforePut", String.valueOf(checkBeforePut) );
        conf.set("com.ohadr.nameNode", nameNode );
        conf.set("com.ohadr.zookeeper", zookeeper );
        conf.set("com.ohadr.zookeeperPort", zookeeperPort );

        JobConf jobConf = new JobConf(conf, App.class);
        Properties properties = AppProps.appProps().setName("id-generator").setVersion("1.0").buildProperties(jobConf);

        FlowConnector flowConnector = new HadoopFlowConnector(properties);

        //create source and sink
        Tap sessionTap = new Hfs(new TextDelimited(false, HBaseDAL.DELIMITER), filePath);
//        sessionTap.getScheme().setSourceFields( new Fields( "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10",
//                "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9", "b10",
//                "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10",
//                "d1", "d2", "d3", "d4", "d5" ) );
        sessionTap.getScheme().setSourceFields( new Fields( "a1", "b", "c" ) );
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
}
