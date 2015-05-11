package com.nice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Created by ohadre on 11/05/2015.
 */
public class Config {

    private static Config theInstance = new Config();

    private String nameNode;

    public static Config instance()
    {
        return theInstance;
    }

    public void setNameNode(String newNameNode)
    {
        nameNode = newNameNode;
    }

    /**
     * This method sets the HBase configuration
     * @return - configuration
     */
    public Configuration getConfiguration() {
        Configuration hBaseConfig = HBaseConfiguration.create();
        hBaseConfig.setInt("timeout", 120000);
        hBaseConfig.set("hbase.master", "*" + nameNode + ":9000*");
        hBaseConfig.set("hbase.zookeeper.quorum", nameNode);
        hBaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
        return hBaseConfig;
    }
}
