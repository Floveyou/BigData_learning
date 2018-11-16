package hadoop.mr.dataskew2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper 程序2
 * 重新设计 Key
 */
public class WCMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * map 函数，被调用过程是通过 while 循环每行调用一次
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 将 value 变为 String 格式
        String line = value.toString();
        // 切割一行文本分为 key 和 value
        String[] arr = line.split("\t");

        String word = arr[0];

        Integer count = Integer.parseInt(arr[1]);

        context.write(new Text(word), new IntWritable(count));

    }
}
