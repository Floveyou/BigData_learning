package com.share.hbase.TableFormat;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * @user: share
 * @date: 2019/2/16
 * @description:
 */
public class TableInputMapper extends Mapper<ImmutableBytesWritable, Result,Text,IntWritable> {


    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        List<Cell> cells = value.listCells();
        for (Cell cell : cells) {
            String line = new String(CellUtil.cloneValue(cell));
            String[] arr = line.split(" ");
            for(String word : arr){
                context.write(new Text(word),new IntWritable(1));
            }
            String s = new String(key.get());
            System.out.println(s);
        }
    }
}