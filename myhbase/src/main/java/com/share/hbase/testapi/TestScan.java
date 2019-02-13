package com.share.hbase.testapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.List;


/**
 * @user: share
 * @date: 2019/2/13
 * @description: 测试 HBase 的两种扫描方式
 */
public class TestScan {

    /**
     * get扫描
     */
    @Test
    public void getData() throws Exception {

        // 初始化 HBase 的conf
        Configuration conf = HBaseConfiguration.create();

        // 通过连接工厂创建连接
        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = conn.getTable(TableName.valueOf("test:t1"));

        Get get = new Get(Bytes.toBytes("row1"));

        Result result = table.get(get);

        List<Cell> cells = result.listCells();

        for (Cell cell : cells) {
            String row = Bytes.toString(CellUtil.cloneRow(cell));
            String cf = Bytes.toString(CellUtil.cloneFamily(cell));
            String col = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));

            System.out.println(row + "/" + cf + "/" + col + "/" + value);
        }

    }

    /**
     * scan 扫描
     */
    @Test
    public void scanData() throws Exception {


        // 初始化 HBase 的conf
        Configuration conf = HBaseConfiguration.create();

        // 通过连接工厂创建连接
        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = conn.getTable(TableName.valueOf("test:t1"));

        Scan scan = new Scan();

        ResultScanner scanner = table.getScanner(scan);

        for (Result rs : scanner) {
            List<Cell> cells = rs.listCells();

            for (Cell cell : cells) {
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                String col = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                System.out.println(row + "/" + cf + "/" + col + "/" + value);
            }
        }
    }
}
