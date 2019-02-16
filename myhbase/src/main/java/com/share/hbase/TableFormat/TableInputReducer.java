package com.share.hbase.TableFormat;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @user: share
 * @date: 2019/2/16
 * @description:
 */
public class TableInputReducer extends Reducer<Text, IntWritable, NullWritable, Put> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        Put put = new Put(Bytes.toBytes(key.toString()));
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("count"), Bytes.toBytes(sum +""));

        context.write(NullWritable.get(),put);

    }
}
