package hadoop.mr.userdraw;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * UserDraw 的 Reducer 程序
 * 输出 Key   null
 * 输出 Value phoneNum|appId|sum(durationTime)
 */
public class UserDrawReducer extends Reducer<Text, IntWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        String newVal = key.toString() + "|" + sum;
        context.write(null, new Text(newVal));
    }
}
