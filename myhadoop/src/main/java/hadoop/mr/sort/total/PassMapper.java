package hadoop.mr.sort.total;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * mapper 类
 * 对原始数据进行预处理
 */
public class PassMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 将 value 变为 String 格式
        String line = value.toString();

        // 将一行文本进行截串
        String[] arr = line.split("\t");

        // 过滤不符合规范的数据
        if (arr.length >= 3) {

            String pass = arr[2];
            if (pass != null) {
                context.write(new Text(pass), new IntWritable(1));
            }
        }
    }
}
