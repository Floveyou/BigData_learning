package com.share.hbase.TableFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * @user: share
 * @date: 2019/2/16
 * @description:HBase实现WC
 * 建两个表
 * create 'test:t3','f1','f2'
 * create 'test:wc','f1','f2'
 *
 * put 'test:t3','row1','f1:line','are you ok'
 * put 'test:t3','row2','f1:line','hello thank you thank you very much'
 */
public class TableInputApp {
    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        conf.set(TableInputFormat.INPUT_TABLE,"test:t2");
        conf.set(TableOutputFormat.OUTPUT_TABLE,"test:wc");

        Job job = Job.getInstance(conf);

        job.setJobName("TableFormat");
        job.setJarByClass(TableInputApp.class);

        job.setMapperClass(TableInputMapper.class);
        job.setReducerClass(TableInputReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        job.waitForCompletion(true);
    }
}
