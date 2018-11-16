package hadoop.mr.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper 程序
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * map 函数，被调用过程是通过 while 循环每行调用一次
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 将 value 变为 String 格式
        String line = value.toString();
        // 将一行文本进行截串
        String[] arr = line.split(" ");

        for (String word : arr) {
            context.write(new Text(word), new IntWritable(1));
        }

    }
}
