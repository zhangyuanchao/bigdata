package com.crius.hbase.crud;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBasePage {

    private Connection connection;

    public HBasePage(Connection connection) {
        this.connection = connection;
    }

    public ResultScanner getPageData(String startRow, int pageNum) throws IOException {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"));
        // 设置当前查询的起始位置
        if (startRow != null && startRow.length() > 0) {
            scan.withStartRow(Bytes.toBytes(startRow));
        }
        PageFilter pageFilter = new PageFilter(pageNum);
        scan.setFilter(pageFilter);
        Table table = connection.getTable(TableName.valueOf("t1"));
        ResultScanner resultScanner = table.getScanner(scan);
        return resultScanner;
    }
}
