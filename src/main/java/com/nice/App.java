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
import com.ccih.base.model.RetryStrategy;
import com.ccih.base.model.configuration.ConfigurationValue;
import com.ccih.base.model.configuration.ConfigurationValueDTO;
import com.ccih.base.model.configuration.ConfigurationValueInterface;
import com.ccih.base.service.ConfigurationServiceClient;
import com.ccih.base.service.ConfigurationServiceClientImpl;
import com.ccih.base.service.ServiceUtils;
import com.ccih.common.generators.repositories.HBaseIDRepository;
import com.ccih.common.util.PlatformException;
import com.ccih.common.util.security.TenantHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.Date;
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

    public static final int MAX_BUCKETS = 32;
    private static final String BIGDATA_MIGRATION_IDGENERATION_DATE = "|BIGDATA|Migration|IDGenerationDate|";
    public static String ID_GEN_TABLE_NAME = TenantHelper.getTenantAwareTableName("GeneratedSessionID");
    public static String ID_GEN_TABLE_NAME_CF = "d";
    public static String ID_GEN_TABLE_NAME_QF = "ID";

    private HBaseIDRepository hBaseIDRepository = new HBaseIDRepository();

    @Option(name="-filePath")
    private String filePath = "/";

    @Option(name="-nameNode")
    private static String nameNode = "vbox.localdomain";

    @Option(name="-checkBeforePut")
    private static boolean checkBeforePut = false;

    public static void main(String[] args) throws Throwable
    {
        new App().doMain(args);
    }

    private void doMain(String[] args) throws IOException, PlatformException {
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
        System.out.println("checkBeforePut? " + checkBeforePut);

        String HDFS = "hdfs://" + nameNode + ":9000";
        String JOB_TRACKER = nameNode + ":9001";


        //create table if not exists.
        createHBaseTable();

//        persistTimestamp();



        Configuration conf = new Configuration();
        conf.set("fs.default.name", HDFS);
        conf.set("mapred.job.tracker", JOB_TRACKER);
        conf.set("mapred.map.tasks", "84");

        JobConf jobConf = new JobConf(conf, App.class);
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
        flow.complete();
    }


    private void createHBaseTable() throws IOException, PlatformException
    {
        Configuration hBaseConfig = Config.instance().getConfiguration();
        hBaseIDRepository.configureHBase(hBaseConfig, ID_GEN_TABLE_NAME);
        System.out.println("created table name: " + ID_GEN_TABLE_NAME);
    }

    private void persistTimestamp()
    {
        ConfigurationServiceClient configurationServiceClient =
//                ServiceUtils.getService(ConfigurationServiceClient.class);
                new ConfigurationServiceClientImpl();
        ConfigurationValueDTO valueDTO = new ConfigurationValueDTO();
        valueDTO.setDateValue( new Date(System.currentTimeMillis()) );
        configurationServiceClient.set(BIGDATA_MIGRATION_IDGENERATION_DATE, valueDTO );
    }

    public static String getNameNode() {
        return nameNode;
    }

    public static boolean isCheckBeforePut() {
        return checkBeforePut;
    }
}
