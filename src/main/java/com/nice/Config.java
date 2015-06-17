package com.nice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Created by ohadre on 11/05/2015.
 */
public class Config {

    public static final String CONF__TENANT = "com.ccih.migration.tenant";
    public static final String CONF__CHECK_BEFORE_PUT = "com.ccih.migration.checkBeforePut";
    public static final String CONF__NAME_NODE = "com.ccih.migration.nameNode";
    public static final String CONF__ZOOKEEPER = "com.ccih.migration.zookeeper";
    public static final String CONF__ZOOKEEPER_PORT = "com.ccih.migration.zookeeperPort";

    //private ctor:
    private Config() {}

    private static Config theInstance = new Config();

    public static Config instance()
    {
        return theInstance;
    }

    /**
     * This method sets the HBase configuration
     * @return - configuration
     * @param jobConf - the config of the job (includes params as the zookeeper, namenode etc)
     */
    public Configuration getConfiguration(Configuration jobConf) {

        String zookeeper = jobConf.get(CONF__ZOOKEEPER);
        String zookeeperPort = jobConf.get(CONF__ZOOKEEPER_PORT );
        String nameNode = jobConf.get(CONF__NAME_NODE);

        Configuration hBaseConfig = HBaseConfiguration.create();
        hBaseConfig.setInt("timeout", 120000);
        hBaseConfig.set("hbase.master", "*" + nameNode + ":9000*");
        hBaseConfig.set("hbase.zookeeper.quorum", zookeeper);
        hBaseConfig.set("hbase.zookeeper.property.clientPort", zookeeperPort);
        return hBaseConfig;
    }
}
