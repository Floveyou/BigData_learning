package hadoop.mr.maxtemp0;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区函数，以年份分区
 */
public class YearPartitioner extends Partitioner<IntWritable, IntWritable> {

    public int getPartition(IntWritable key, IntWritable value, int numPartitions) {

        int year = key.get();

        if(year>1960){
            return 1;
        }
        else if(year>1930){
            return 1;
        }
        return 2;
    }

}
