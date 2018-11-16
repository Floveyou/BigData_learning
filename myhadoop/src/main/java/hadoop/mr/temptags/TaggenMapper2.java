package hadoop.mr.temptags;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * TaggenMapper2 程序
 * 将 70611801_干净卫生	4 转换为 70611801 干净卫生_4
 */
public class TaggenMapper2 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split("\t");

        String id = arr[0].split("_")[0];
        String tag = arr[0].split("_")[1];
        String sum = arr[1];

        context.write(new Text(id), new Text(tag + "_" + sum));

    }
}
