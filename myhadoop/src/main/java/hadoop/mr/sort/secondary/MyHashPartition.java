package hadoop.mr.sort.secondary;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义 hash 分区
 */
public class MyHashPartition extends Partitioner<CompKey, NullWritable> {
    public int getPartition(CompKey compKey, NullWritable nullWritable, int numPartitions) {
        String year = compKey.getYear();

        return (year.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
