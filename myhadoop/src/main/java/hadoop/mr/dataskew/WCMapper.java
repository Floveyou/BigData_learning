package hadoop.mr.dataskew;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

/**
 * Mapper 程序
 * 重新设计 Key
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Random r = new Random();
    int i;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取 reduce 的个数
        i = context.getNumReduceTasks();
    }

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

            String newWord = word + "_" + r.nextInt(i);

            context.write(new Text(newWord), new IntWritable(1));
        }

    }
}
