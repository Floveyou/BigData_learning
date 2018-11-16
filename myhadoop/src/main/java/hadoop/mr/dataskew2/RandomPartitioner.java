package hadoop.mr.dataskew2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Random;

/**
 * 随机分区类
 */
public class RandomPartitioner extends Partitioner<Text, IntWritable> {

    Random r = new Random();

    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        return r.nextInt(numPartitions);
    }
}
