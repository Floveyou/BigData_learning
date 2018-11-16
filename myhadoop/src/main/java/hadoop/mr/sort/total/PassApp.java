package hadoop.mr.sort.total;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 对密码进行全排序
 * 通过自定义分区实现全排序
 */
public class PassApp {
    public static void main(String[] args) throws Exception {
        // 初始化配置文件
        Configuration conf = new Configuration();

        // 仅在本地开发时使用
        conf.set("fs.defaultFS", "file:///");

        // 初始化文件系统
        FileSystem fs = FileSystem.get(conf);

        // 通过配置文件初始化 job
        Job job = Job.getInstance(conf);

        // 设置 job 名称
        job.setJobName("pass count");

        // job 入口函数类
        job.setJarByClass(PassApp.class);

        // 设置 mapper 类
        job.setMapperClass(PassMapper.class);

        // 设置 reducer0 类
        job.setReducerClass(PassReducer.class);

        // 设置 partition 类
        job.setPartitionerClass(PassPartition.class);

        // 设置 combiner 类
//        job.setCombinerClass(PassReducer.class);

        // 设置分区数量
        job.setNumReduceTasks(3);

        // 设置 map 的输出 K-V 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置 reduce 的输出 K-V 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入路径和输出路径
        Path pin = new Path("E:/file/duowan_user.txt");
        Path pout = new Path("E:/test/wc/out");
//        Path pin = new Path(args[0]);
//        Path pout = new Path(args[1]);
        FileInputFormat.addInputPath(job, pin);
        FileOutputFormat.setOutputPath(job, pout);

        // 判断输出路径是否已经存在，若存在则删除
        if (fs.exists(pout)) {
            fs.delete(pout, true);
        }

        // 执行 job
        job.waitForCompletion(true);


    }
}
