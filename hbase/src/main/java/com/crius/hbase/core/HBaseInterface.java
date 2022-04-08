package com.crius.hbase.core;

import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.util.List;

public interface HBaseInterface {

    // 查询所有表
    TableName[] listAllTables() throws Exception;

    // 创建表, 传参:表名和列簇的名字
    void createTable(String tableName, String[] families) throws Exception;

    // 创建表, 传参:封装好的多个列簇
    void createTable(TableDescriptor tds) throws Exception;

    // 创建表, 传参:表名和封装好的多个列簇
    void createTable(String tableName, List<ColumnFamilyDescriptor> cfds) throws Exception;

    // 查看表的列簇属性
    ColumnFamilyDescriptor[] descTable(String tableName) throws Exception;

    // 判断表存不存在
    boolean existTable(String tableName) throws Exception;

    // disable表
    void disableTable(String tableName) throws Exception;

    // drop表
    void dropTable(String tableName) throws Exception;

    // 修改表(增加和删除)
    void modifyTable(String tableName) throws Exception;

    void modifyTable(String tableName, String[] addColumn, String[] removeColumn) throws Exception;

    void modifyTable(String tableName, ColumnFamilyDescriptor cfds) throws Exception;

    // 添加或者修改数据
    void addData(String tableName, String rowKey, String[] column, String[] value) throws Exception;

    void putData(String tableName, String rowKey, String familyName, String columnName, String value) throws Exception;

    void putData(String tableName, String rowKey, String familyName, String columnName, String value, long timestamp) throws Exception;

    void putData(Put put) throws Exception;

    void putData(List<Put> putList) throws Exception;

    // 根据rowKey查询数据
    Result getResult(String tableName, String rowKey) throws Exception;

    Result getResult(String tableName, String rowKey, String familyName) throws Exception;

    Result getResult(String tableName, String rowKey, String familyName, String columnName) throws Exception;

    // 查询指定version
    Result getResultByVersion(String tableName, String rowKey, String familyName, String columnName, int versions) throws Exception;

    // scan全表数据
    ResultScanner getResultScanner(String tableName) throws Exception;

    ResultScanner getResultScanner(String tableName, Scan scan) throws Exception;

    ResultScanner getResultScanner(String tableName, String columnFamily, String qualifier) throws Exception;

    // 删除数据(指定的列)
    void deleteColumn(String tableName, String rowKey) throws Exception;

    void deleteColumn(String tableName, String rowKey, String familyName) throws Exception;

    void deleteColumn(String tableName, String rowKey, String familyName, String columnName) throws Exception;
}
