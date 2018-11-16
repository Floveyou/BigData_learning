package hadoop.mr.top10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeSet;

/**
 * TopMapper2 程序
 * 获取每个 Map 的前10
 */
public class TopMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {

    int top;
    TreeSet<CompKey> ts;

    // 加载获取前几的参数
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        top = Integer.parseInt(context.getConfiguration().get("num.top"));
        ts = new TreeSet<CompKey>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split("\t");
        String pwd = arr[0];
        int sum = Integer.parseInt(arr[1]);

        CompKey ck = new CompKey(pwd, sum);
        ts.add(ck);

        // 获取前十
        if (ts.size() > top) {
            ts.remove(ts.last());
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
