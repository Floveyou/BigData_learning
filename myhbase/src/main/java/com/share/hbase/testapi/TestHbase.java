package com.share.hbase.testapi;

import jdk.nashorn.internal.runtime.linker.NashornCallSiteDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;


/**
 * @user: share
 * @date: 2019/2/13
 * @description: 测试 HBase API 的使用
 */
public class TestHbase {

    /**
     * 测试新建namespace
     */
    @Test
    public void createNS() throws Exception {

        // 初始化 HBase 的conf
        Configuration conf = HBaseConfiguration.create();

        // 通过连接工厂创建连接
        Connection conn = ConnectionFactory.createConnection(conf);

        // 获取 HBase 管理员
        Admin admin = conn.getAdmin();

        // 通过构建器模式，创建 namespace 描述符
        NamespaceDescriptor nsd = NamespaceDescriptor.create("test").build();

        admin.createNamespace(nsd);

        admin.close();

        conn.close();
    }

    /**
     * 测试新建table
     */
    @Test
    public void createTable() throws Exception {

        // 初始化 HBase 的conf
        Configuration conf = HBaseConfiguration.create();

        // 通过连接工厂创建连接
        Connection conn = ConnectionFactory.createConnection(conf);

        // 获取 HBase 管理员
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("test:t1");

        // 通过构建器模式，创建 table 描述符
        HTableDescriptor htd = new HTableDescriptor(tableName);

        htd.addFamily(new HColumnDescriptor("f1"));
        htd.addFamily(new HColumnDescriptor("f2"));

        admin.createTable(htd);

        admin.close();

        conn.close();
    }

    /**
     * 插入数据
     */
    @Test
    public void putData() throws Exception {

        // 初始化 HBase 的conf
        Configuration conf = HBaseConfiguration.create();

        // 通过连接工厂创建连接
        Connection conn = ConnectionFactory.createConnection(conf);

        HTable table = (HTable) conn.getTable(TableName.valueOf("test:t1"));

        // 设置自动刷写为false
        table.setAutoFlush(false, false);

        long start = System.currentTimeMillis();

        // Bytes.toBytes(）可以将任意类型转换成字节数组
        Put put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("tom"));
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"), Bytes.toBytes("18"));
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("sex"), Bytes.toBytes("male"));

        table.put(put);
        table.close();
        conn.close();

        System.out.println(System.currentTimeMillis() - start);

    }

    /**
     * 批量插入数据
     */
    @Test
    public void batchPut() throws Exception {

        // 初始化 HBase 的conf
        Configuration conf = HBaseConfiguration.create();

        // 通过连接工厂创建连接
        Connection conn = ConnectionFactory.createConnection(conf);

        HTable table = (HTable) conn.getTable(TableName.valueOf("test:t1"));

        // 设置自动刷写为false
        table.setAutoFlush(false, false);

        // put列表
        List<Put> list = new LinkedList<Put>();

        DecimalFormat df = new DecimalFormat("000");

        long start = System.currentTimeMillis();

        for (int i = 1; i < 10000; i++) {
            String str = df.format(i);
            // Bytes.toBytes(）可以将任意类型转换成字节数组
            Put put = new Put(Bytes.toBytes("row" + str));
            // 跳过WAL写入
            //put.setDurability(Durability.SKIP_WAL);
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("tom" + i));
            //put.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("id"), Bytes.toBytes(str));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"), Bytes.toBytes(df.format(i % 70)));
            list.add(put);
        }

        // 数据通过put或者delete对象进行发送，发送时以mutator形式
        // 每次发送将对象封装成linkedList，再进行一次flush，即一次rpc通信
        table.put(list);
        table.flushCommits();

        table.close();
        conn.close();

        System.out.println(System.currentTimeMillis() - start);

    }

    /**
     * 删除数据
     */
    @Test
    public void delData() throws Exception {

        // 初始化 HBase 的conf
        Configuration conf = HBaseConfiguration.create();

        // 通过连接工厂创建连接
        Connection conn = ConnectionFactory.createConnection(conf);

        HTable table = (HTable) conn.getTable(TableName.valueOf("test:t1"));

        // 设置自动刷写为false
        table.setAutoFlush(false, false);

        long start = System.currentTimeMillis();

        // Bytes.toBytes(）可以将任意类型转换成字节数组
        Delete del = new Delete(Bytes.toBytes("row1"));
        del.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"));

        table.delete(del);

        table.close();
        conn.close();

        System.out.println(System.currentTimeMillis() - start);

    }
}
