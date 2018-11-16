package hadoop.mr.top10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

/**
 * 获取所有的 Map 中的前十
 */
public class TopReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {

    int top;
    TreeSet<CompKey> ts;

    // 加载获取前几的参数
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        top = Integer.parseInt(context.getConfiguration().get("num.top"));
        ts = new TreeSet<CompKey>();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value : values) {
            String pwd = key.toString();
            int sum = value.get();

            CompKey ck = new CompKey(pwd, sum);
            ts.add(ck);

            // 获取前十
            if (ts.size() > top) {
                ts.remove(ts.last());
            }
        }
    }

    // 当 map 完成 且 ts 满了之后，将所有的 ts 进行写出
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (CompKey ck : ts) {

            String pass = ck.getPwd();
            int sum = ck.getSum();

            context.write(new Text(pass), new IntWritable(sum));
        }
    }
}
