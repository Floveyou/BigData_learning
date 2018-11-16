package hadoop.mr.join.reducer0;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @user: share
 * @date: 2018/9/21
 * @description:
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

        if (fileName.contains("cust")) {
            int flag = 1;
            String cusLine = value.toString();
            String[] arr = cusLine.split(",");
            if (arr.length == 3) {
                String id = arr[0];
                CompKey ck = new CompKey(flag, id);
                context.write(ck, new Text(cusLine));
            }
        }

        else {
            int flag = 2;
            String orderLine = value.toString();
            String[] arr = orderLine.split(",");
            if(arr.length == 4){
                String id = arr[3];
                CompKey ck = new CompKey(flag,id);
                context.write(ck,new Text(orderLine));
            }
        }

    }


}
