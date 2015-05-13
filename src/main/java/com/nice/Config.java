package com.nice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Created by ohadre on 11/05/2015.
 */
public class Config {

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

        String zookeeper = jobConf.get("com.ohadr.zookeeper");
        String zookeeperPort = jobConf.get("com.ohadr.zookeeperPort" );
        String nameNode = jobConf.get("com.ohadr.nameNode");

        Configuration hBaseConfig = HBaseConfiguration.create();
        hBaseConfig.setInt("timeout", 120000);
        hBaseConfig.set("hbase.master", "*" + nameNode + ":9000*");
        hBaseConfig.set("hbase.zookeeper.quorum", zookeeper);
        hBaseConfig.set("hbase.zookeeper.property.clientPort", zookeeperPort);
        return hBaseConfig;
    }
}
