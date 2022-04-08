package com.crius.hbase.crud;

import com.crius.hbase.core.EnvironmentConfiguration;
import com.crius.hbase.core.HBaseCore;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

import java.io.IOException;

public class HBaseCrud {
    public static void main(String[] args) throws Exception {
        EnvironmentConfiguration configuration = new EnvironmentConfiguration();
        HBaseCore hbaseCore = new HBaseCore(configuration);
        TableName[] tableNames = hbaseCore.listAllTables();
        for (TableName tableName : tableNames) {
            System.out.println(tableName.getNameAsString());
        }
    }

    public boolean isTableExist(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        return true;
    }
}
