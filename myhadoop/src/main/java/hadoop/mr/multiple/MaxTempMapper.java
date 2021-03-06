package hadoop.mr.multiple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Mapper 类
 */
public class MaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    MultipleOutputs mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 将 value 变为 String 格式
        String line = value.toString();
        // 获得年份
        String year = line.substring(15, 19);
        // 获得温度
        int temp = Integer.parseInt(line.substring(87, 92));

        // 存在脏数据 9999，所以要将其过滤
        if (temp != 9999) {
            // 输出年份与温度
            context.write(new Text(year), new IntWritable(temp));
        }

        mos.write("text", new Text(year), new IntWritable(temp),"E:/test/wc/text_out");

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
