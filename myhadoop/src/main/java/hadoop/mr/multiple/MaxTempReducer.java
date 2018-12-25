package hadoop.mr.multiple;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Reducer 类
 */
public class MaxTempReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    MultipleOutputs mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Integer max = Integer.MIN_VALUE;

        // 得到最大值
        for (IntWritable value : values) {
            max = Math.max(max, value.get());
        }

        // 输出年份与最大温度
        context.write(key, new DoubleWritable(max / 10.0));

        mos.write("seq", key, new DoubleWritable(max / 10.0), "E:/test/wc/seq_out");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
