package hadoop.mr.join.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Reduce Join 的 Mapper 程序
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, CompKey, Text> {

    String fileName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 通过切片得到文件名
        FileSplit split = (FileSplit) context.getInputSplit();
        fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (fileName.contains("customers")) {
            int flag = 1;
            String cusLine = value.toString();
            String[] arr = cusLine.split("\t");
            if (arr.length == 3) {
                String id = arr[0];
                CompKey ck = new CompKey(flag, id);
                context.write(ck, new Text(cusLine));
            }
        } else {
            int flag = 2;
            String orderLine = value.toString();
            String[] arr = orderLine.split("\t");
            if (arr.length == 4) {
                String id = arr[3];
                CompKey ck = new CompKey(flag, id);
                context.write(ck, new Text(orderLine));
            }
        }
    }
}
