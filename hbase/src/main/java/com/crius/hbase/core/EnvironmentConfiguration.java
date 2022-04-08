package com.crius.hbase.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class EnvironmentConfiguration {

    private Configuration conf;

    public EnvironmentConfiguration() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1,node2,node3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public Configuration getConf() {
        return this.conf;
    }
}
