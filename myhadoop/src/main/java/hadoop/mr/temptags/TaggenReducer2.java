package hadoop.mr.temptags;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

/**
 * TaggenReducer 程序
 * 将 70611801 干净卫生_4  和 70611801 服务热情_1 聚合为
 * 70611801 干净卫生_4,服务热情_1
 */
public class TaggenReducer2 extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // 初始化 TreeSet 进行排序
        TreeSet<CompKey> ts = new TreeSet<CompKey>();

        // 拼串过程，初始化 StringBuffer
        StringBuffer sb = new StringBuffer();

        // 迭代所有 value 并将其 K-V 组装成 compKey 发送到 TreeSet
        for (Text value : values) {
            String[] arr = value.toString().split("_");
            ts.add(new CompKey(arr[0], Integer.parseInt(arr[1])));
        }

        //迭代TreeSet并进行拼串
        for (CompKey compKey : ts) {
            sb.append(compKey.toString() + ",");
        }

        String newValue1 = sb.toString();
        String newValue2 = newValue1.substring(0, newValue1.length() - 1);

        context.write(new Text(key), new Text(newValue2));

        // 清空 StringBuffer
        sb.setLength(0);

        ts.clear();
    }
}
