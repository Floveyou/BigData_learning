package hadoop.mr.maxtemp0;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @user: share
 */
public class App {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        // 通过配置文件初始化job
        Job job = Job.getInstance(conf);
        // 设置job名称
        job.setJobName("maxTemp");

        // job入口函数类
        job.setJarByClass(App.class);

        // 设置mapper类
        job.setMapperClass(MaxTempMapper.class);

        // 设置reducer类
        job.setReducerClass(MaxTempReducer.class);

        // 设置分区类
        job.setPartitionerClass(YearPartitioner.class);

        // 设置map的输出k-v类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置reduce的输出k-v类型
        job.setOutputValueClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入路径
        FileInputFormat.addInputPath(job, new Path("file:///F:\\Archives\\OB\\Xu\\x_hadoop\\temp3.dat"));

        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path("file:///F:\\Archives\\OB\\Xu\\x_hadoop\\out"));

        //设置最大切片大小
        FileInputFormat.setMaxInputSplitSize(job,30000);

        // 设置三个reduce
        job.setNumReduceTasks(3);

        // 执行job
        job.waitForCompletion(true);
    }
}
