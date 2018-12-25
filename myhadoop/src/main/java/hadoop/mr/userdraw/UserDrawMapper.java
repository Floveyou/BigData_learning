package hadoop.mr.userdraw;

import hadoop.mr.userdraw.util.ConfUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * UserDraw 的 Mapper 程序
 * 输出 Key   phoneNum|appId
 * 输出 Value durationTime
 */
public class UserDrawMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    ConfUtil confUtil = new ConfUtil();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] arr = line.split(confUtil.separator);

        // 获取手机号
        String phoneNum = arr[Integer.parseInt(confUtil.phone)];
        // 获取 appid
        String appId = arr[Integer.parseInt(confUtil.appid)];
        // 获取时间戳
        String timeStamp = arr[Integer.parseInt(confUtil.time)];
        // 获取使用时长
        String durationTime = arr[Integer.parseInt(confUtil.duration)];

        if (phoneNum != null && appId != null && !phoneNum.equals("") && !appId.equals("")) {

            Text newKey = new Text(phoneNum + "|" + appId);
            IntWritable newVal = new IntWritable(Integer.parseInt(durationTime));

            context.write(newKey, newVal);
        }
    }
}
