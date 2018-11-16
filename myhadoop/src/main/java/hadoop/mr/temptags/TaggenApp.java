package hadoop.mr.temptags;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 标签生成 App
 * 第一次 MR 的结果 70611801_干净卫生	4
 * 第二次 MR 的结果 70611801 干净卫生_4,服务热情_1
 */
public class TaggenApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 初始化配置文件
        Configuration conf = new Configuration();

        // 仅在本地开发时使用
        conf.set("fs.defaultFS", "file:///");

        // 初始化文件系统
        FileSystem fs = FileSystem.get(conf);

        // 通过配置文件初始化 job
        Job job = Job.getInstance(conf);

        // 设置 job 名称
        job.setJobName("Taggen1");

        // job 入口函数类
        job.setJarByClass(TaggenApp.class);

        // 设置 mapper 类
        job.setMapperClass(TaggenMapper.class);

        // 设置 reducer0 类
        job.setReducerClass(TaggenReducer.class);

        // 设置分区数量
//        job.setNumReduceTasks(3);

        // 设置 map 的输出 K-V 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置 reduce 的输出 K-V 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入路径和输出路径
        Path pin = new Path("E:/file/temptags.txt");
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

        // 进行二次 MR
        if (b) {
            // 通过配置文件初始化 job2
            Job job2 = Job.getInstance(conf);

            // 设置 job2 名称
            job2.setJobName("Taggen2");

            // job2 入口函数类
            job2.setJarByClass(TaggenApp.class);

            // 设置 mapper 类
            job2.setMapperClass(TaggenMapper2.class);

            // 设置 reducer0 类
            job2.setReducerClass(TaggenReducer2.class);

            // 设置分区数量
//        job2.setNumReduceTasks(3);

            // 设置 map 的输出 K-V 类型
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);

            // 设置 reduce 的输出 K-V 类型
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            // 设置输入路径和输出路径
            Path pin2 = new Path("E:/test/wc/out");
            Path pout2 = new Path("E:/test/wc/out2");
//        Path pin2 = new Path(args[0]);
//        Path pout2 = new Path(args[1]);
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
