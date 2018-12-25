package hadoop.mr.userdraw;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * UserDraw 的 Mapper2 程序
 */
public class UserDrawMapper2 extends Mapper<LongWritable,Text,Text,CompKey> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] arr = line.split("\\|");

        String phone = arr[0];

        CompKey ck = new CompKey(arr[1], Integer.parseInt(arr[2]));

        context.write(new Text(phone), ck);


    }
}

/**
 * +/rmMLtMV+s+gXTDoOaoxQ==|10005|824
 * +/rmMLtMV+s+gXTDoOaoxQ==|220499|98
 * +/rmMLtMV+s+gXTDoOaoxQ==|70093|75610
 *
 * AppTab
 * 10005|微信|0.001|0.001|0|0.2|0.3|0.2|0.3
 * 220499|搜狐浏览器|0.001|0.001|0.001|0.002|0.002|0.002|0.003
 * 70093|豌豆荚|0.6|0.4|0.001|0.002|0.002|0.002|0.003
 *
 * 男|女|年龄1|年龄2|年龄3|年龄4|年龄5
 *
 * 824x0.001 | 824x0.001 | 824x0 | 824x0.2 | 824x0.3 | 824x0.2 |824x0.3
 * 98x0.001 | 98x0.001 |98x0.001 |98x0.002 |98x0.002 |98x0.002 |98x0.003
 * 75610x0.6 | 75610x0.4 | xxxxxx
 *
 *  sum1    | sum2  | sum3 | .....  => mos
 *
 *  50000  |  30000 | xxxxxx ...
 *
 *  0.625 | 0.375
 *
 *
 *  可以把10005|824 => MapWritable<Text,IntWritable>
 *
 *  appTab<10005,10005|微信|0.001|0.001|0|0.2|0.3|0.2|0.3>
 *  arr[7] = {0,0,0,0,0,0,0}
 *  for(){
 *      MapWritable<10005, 824>
 *      getKey()
 *      appTab.get<10005> => 10005|微信|0.001|0.001|0|0.2|0.3|0.2|0.3
 *
 *      arr[0] += 0.001 x 824
 *      arr[1] +=
 *      ......
 *  }
 *
 *  mos.write(phone, arr1|arr2|arr3|arr4|arr5|arr6|arr7)
 *
 *
 *  id|arr1/(arr1+arr2) | arr2/(arr1+arr2) | .......
 *
 */
