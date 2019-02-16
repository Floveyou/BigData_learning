package com.share.hbase.bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

/**
 * @user: share
 * @date: 2019/2/16
 * @description: BulkLoad批量上传数据到HBase表中
 */
public class BulkLoads {
    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();

        Connection conn = ConnectionFactory.createConnection(conf);

        //将数据放在hdfs上
        //以region为单位，将数据迁移到另外一个表中并加载
        //注意，被加载的路径必须是hdfs路径
        LoadIncrementalHFiles load = new LoadIncrementalHFiles(conf);
        load.doBulkLoad(new Path("/user/hbase/data/test/t1/e45f8a3a4db921d24425601b9e0bb446"), conn.getAdmin(),
                conn.getTable(TableName.valueOf("test:wc")), conn.getRegionLocator(TableName.valueOf("test:wc")));
        conn.close();
    }
}
