package com.share.hbase.hfile;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @user: share
 * @date: 2019/2/16
 * @description:
 */
public class HFileMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] arr = line.split("\t");

        if(arr.length >= 3){

            String pass = arr[2];
            if(pass != null){
                context.write(new Text(pass), new IntWritable(1));
            }
        }
    }
}