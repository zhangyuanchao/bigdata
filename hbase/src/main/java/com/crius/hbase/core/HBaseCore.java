package com.crius.hbase.core;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseCore implements HBaseInterface {
    private Connection connection;
    private Admin admin;

    public HBaseCore (EnvironmentConfiguration configuration) throws IOException {
        //在HBase中管理、访问表需要先创建 HBaseAdmin 对象
        connection = ConnectionFactory.createConnection(configuration.getConf());
        admin = connection.getAdmin();
    }

    public TableName[] listAllTables() throws Exception {
        TableName[] tableNames = admin.listTableNames();
        return tableNames;
    }

    public void createTable(String tableName, String[] families) throws Exception {
        if (tableName == null || tableName.length() == 0 || families.length == 0) {
            return;
        }
        List<ColumnFamilyDescriptor> descriptors = new ArrayList<ColumnFamilyDescriptor>(families.length);
        ColumnFamilyDescriptorBuilder builder = null;
        for (String family : families) {
            builder = ColumnFamilyDescriptorBuilder.newBuilder(family.getBytes());
            builder.setMaxVersions(3);
            descriptors.add(builder.build());
        }
        TableName table = TableName.valueOf(tableName);
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(table).setColumnFamilies(descriptors).build();
        admin.createTable(tableDescriptor);
    }

    public void createTable(TableDescriptor tds) throws Exception {
        admin.createTable(tds);
    }

    public void createTable(String tableName, List<ColumnFamilyDescriptor> cfds) throws Exception {
        TableName table = TableName.valueOf(tableName);
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(table).setColumnFamilies(cfds).build();
        admin.createTable(tableDescriptor);
    }

    public ColumnFamilyDescriptor[] descTable(String tableName) throws Exception {
        TableDescriptor tableDescriptor = admin.getDescriptor(TableName.valueOf(tableName));
        ColumnFamilyDescriptor[] descriptors = tableDescriptor.getColumnFamilies();
        return descriptors;
    }

    public boolean existTable(String tableName) throws Exception {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public void disableTable(String tableName) throws Exception {
        admin.disableTable(TableName.valueOf(tableName));
    }

    public void dropTable(String tableName) throws Exception {
        admin.deleteTable(TableName.valueOf(tableName));
    }

    public void modifyTable(String tableName) throws Exception {
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName)).build();
        admin.modifyTable(tableDescriptor);
    }

    public void modifyTable(String tableName, String[] addColumn, String[] removeColumn) throws Exception {
        List<ColumnFamilyDescriptor> addFamilyDescriptors = new ArrayList<ColumnFamilyDescriptor>(addColumn.length);
        List<ColumnFamilyDescriptor> removeFamilyDescriptors = new ArrayList<ColumnFamilyDescriptor>(removeColumn.length);
        ColumnFamilyDescriptorBuilder builder = null;
        for (String family : addColumn) {
            builder = ColumnFamilyDescriptorBuilder.newBuilder(family.getBytes());
            addFamilyDescriptors.add(builder.build());
        }
        for (String family : removeColumn) {
            builder = ColumnFamilyDescriptorBuilder.newBuilder(family.getBytes());
            removeFamilyDescriptors.add(builder.build());
        }
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName)).build();
    }

    public void modifyTable(String tableName, ColumnFamilyDescriptor cfds) throws Exception {

    }

    public void addData(String tableName, String rowKey, String[] column, String[] value) throws Exception {

    }

    public void putData(String tableName, String rowKey, String familyName, String columnName, String value) throws Exception {

    }

    public void putData(String tableName, String rowKey, String familyName, String columnName, String value, long timestamp) throws Exception {

    }

    public void putData(Put put) throws Exception {

    }

    public void putData(List<Put> putList) throws Exception {

    }

    public Result getResult(String tableName, String rowKey) throws Exception {
        return null;
    }

    public Result getResult(String tableName, String rowKey, String familyName) throws Exception {
        return null;
    }

    public Result getResult(String tableName, String rowKey, String familyName, String columnName) throws Exception {
        return null;
    }

    public Result getResultByVersion(String tableName, String rowKey, String familyName, String columnName, int versions) throws Exception {
        return null;
    }

    public ResultScanner getResultScanner(String tableName) throws Exception {
        return null;
    }

    public ResultScanner getResultScanner(String tableName, Scan scan) throws Exception {
        return null;
    }

    public ResultScanner getResultScanner(String tableName, String columnFamily, String qualifier) throws Exception {
        return null;
    }

    public void deleteColumn(String tableName, String rowKey) throws Exception {

    }

    public void deleteColumn(String tableName, String rowKey, String familyName) throws Exception {

    }

    public void deleteColumn(String tableName, String rowKey, String familyName, String columnName) throws Exception {

    }
}
