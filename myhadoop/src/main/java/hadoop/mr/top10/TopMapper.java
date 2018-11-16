package hadoop.mr.top10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * TopMapper 程序
 * 得到密码字段
 */
public class TopMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split("\t");

        if (arr.length >= 3) {
            String pwd = arr[2];
            context.write(new Text(pwd),new IntWritable(1));
        }
    }
}
