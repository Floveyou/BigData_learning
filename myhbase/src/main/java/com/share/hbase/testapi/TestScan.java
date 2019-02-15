package com.share.hbase.testapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
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

        Get get = new Get(Bytes.toBytes("row001"));

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


        //设定扫描的起始key和结束key
        scan.setStartRow(Bytes.toBytes("row036"));
        scan.setStopRow(Bytes.toBytes("row050"));


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


    /**
     * scan 扫描优化
     */
    @Test
    public void scanData2() throws Exception {


        // 初始化 HBase 的conf
        Configuration conf = HBaseConfiguration.create();

        // 通过连接工厂创建连接
        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = conn.getTable(TableName.valueOf("test:t1"));

        Scan scan = new Scan();

        scan.setCaching(10);// 十次next，一次rpc请求

        scan.setBatch(100);// 设置一次next缓存列数

        //设定扫描的起始key和结束key
//        scan.setStartRow(Bytes.toBytes("row036"));
//        scan.setStopRow(Bytes.toBytes("row050"));


        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> it = scanner.iterator();

        long start = System.currentTimeMillis();
        while (it.hasNext()) {

            List<Cell> cells = it.next().listCells();

            for (Cell cell : cells) {
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                String col = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                System.out.println(row + "/" + cf + "/" + col + "/" + value);
            }
            System.out.println("=========================================");
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    /**
     * 带缓存查询
     */
    @Test
    public void cacheScan() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Table t = conn.getTable(TableName.valueOf("test:t1"));
        Scan scan = new Scan();
        //切忌全表扫描，设置起始结束行
        scan.setStartRow(Bytes.toBytes("row001"));
        scan.setStopRow(Bytes.toBytes("row100"));
        System.out.println(scan.getCaching());
        scan.setCaching(10);

        ResultScanner scanner = t.getScanner(scan);

        Iterator<Result> it = scanner.iterator();
        long start = System.currentTimeMillis();
        int index = 0;
        while (it.hasNext()) {
            it.next().getRow();
            //System.out.println(index ++);
        }
        System.out.println(System.currentTimeMillis() - start);
        t.close();
        conn.close();
    }

    /**
     * 批次查询
     */
    @Test
    public void batchScan() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Table t = conn.getTable(TableName.valueOf("test:t1"));
        Scan scan = new Scan();
        scan.setBatch(0);
        Iterator<Result> r = t.getScanner(scan).iterator();
        while (r.hasNext()) {
            r.next().getRow();
            //System.out.println(index ++);
        }
        t.close();
        conn.close();
    }

}
