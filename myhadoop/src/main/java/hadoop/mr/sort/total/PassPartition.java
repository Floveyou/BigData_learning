package hadoop.mr.sort.total;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * 自定义分区实现全排序
 */
public class PassPartition extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {

        String key = text.toString();
        if (key.compareTo("9") < 0) {
            return 0;
        }
        if (key.compareTo("f") < 0) {
            return 1;
        }
        else return 2;

    }
}
