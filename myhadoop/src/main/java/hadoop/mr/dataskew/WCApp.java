package hadoop.mr.dataskew;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 解决数据倾斜
 */
public class WCApp {
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
        job.setJobName("data skew");

        // job 入口函数类
        job.setJarByClass(WCApp.class);

        // 设置 mapper 类
        job.setMapperClass(WCMapper.class);

        // 设置 reducer0 类
        job.setReducerClass(WCReducer.class);

        // 设置分区数量
        job.setNumReduceTasks(3);

        // 设置 map 的输出 K-V 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置 reduce 的输出 K-V 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入路径和输出路径
        Path pin = new Path("E:/test/wc/dataskew.txt");
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
        boolean b = job.waitForCompletion(true);

        if (b) {
            // 通过配置文件初始化 job
            Job job2 = Job.getInstance(conf);

            // 设置 job 名称
            job2.setJobName("data skew2");

            // job 入口函数类
            job2.setJarByClass(WCApp.class);

            // 设置 mapper 类
            job2.setMapperClass(WCMapper2.class);

            // 设置 reducer0 类
            job2.setReducerClass(WCReducer2.class);

            // 设置分区数量
//            job2.setNumReduceTasks(3);

            // 设置 map 的输出 K-V 类型
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(IntWritable.class);

            // 设置 reduce 的输出 K-V 类型
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);

            // 设置输入路径和输出路径
            Path pin2 = new Path("E:/test/wc/out");
            Path pout2 = new Path("E:/test/wc/out2");
//        Path pin = new Path(args[0]);
//        Path pout = new Path(args[1]);
            FileInputFormat.addInputPath(job2, pin2);
            FileOutputFormat.setOutputPath(job2, pout2);

            // 判断输出路径是否已经存在，若存在则删除
            if (fs.exists(pout2)) {
                fs.delete(pout2, true);
            }

            // 执行 job
            job2.waitForCompletion(true);
        }
    }
}
