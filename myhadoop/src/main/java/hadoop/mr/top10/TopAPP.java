package hadoop.mr.top10;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 密码统计 Top10
 * M1 进行预处理操作和 Combiner 预聚合
 * R1 进行聚合
 * M2 统计 Top10
 * R2 聚合 Top10
 */
public class TopAPP {
    public static void main(String[] args) throws Exception {
        // 初始化配置文件
        Configuration conf = new Configuration();

        // 仅在本地开发时使用
        conf.set("fs.defaultFS", "file:///");

        // 设置取前几
//        conf.set("num.top", args[3]);
        conf.set("num.top", "10");

        // 初始化文件系统
        FileSystem fs = FileSystem.get(conf);

        // 通过配置文件初始化 job
        Job job = Job.getInstance(conf);

        // 设置 job 名称
        job.setJobName("Top10 pwd 1");

        // job 入口函数类
        job.setJarByClass(TopAPP.class);

        // 设置 mapper 类
        job.setMapperClass(TopMapper.class);

        // 设置 reducer 类
        job.setReducerClass(TopReducer.class);

        // 设置 combiner 类
        job.setCombinerClass(TopReducer.class);

        // 设置 map 的输出 K-V 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置 reduce 的输出 K-V 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置 reduce 分区数量
        job.setNumReduceTasks(1);

        // 新建输入输出路径
        Path pin = new Path("E:/file/duowan_user.txt");
        Path pout = new Path("E:/test/wc/out");

        // 打包后自定义输入输出路径
//        Path pin = new Path(args[0]);
//        Path pout = new Path(args[1]);

        // 设置输入路径和输出路径
        FileInputFormat.addInputPath(job, pin);
        FileOutputFormat.setOutputPath(job, pout);

        // 判断输出路径是否已经存在，若存在则删除
        if (fs.exists(pout)) {
            fs.delete(pout, true);
        }

        // 执行 job
        boolean b = job.waitForCompletion(true);

        // 第二次 MR
        if (b) {
            // 通过配置文件初始化 job
            Job job2 = Job.getInstance(conf);

            // 设置 job 名称
            job2.setJobName("Top10 pwd 2");

            // job 入口函数类
            job2.setJarByClass(TopAPP.class);

            // 设置 mapper 类
            job2.setMapperClass(TopMapper2.class);

            // 设置 reducer 类
            job2.setReducerClass(TopReducer2.class);

            // 设置 combiner 类
//            job2.setCombinerClass(TopReducer.class);

            // 设置 map 的输出 K-V 类型
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(IntWritable.class);

            // 设置 reduce 的输出 K-V 类型
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);

            // 设置 reduce 分区数量
            job2.setNumReduceTasks(1);

            // 新建输入输出路径
            Path pin2 = new Path("E:/test/wc/out");
            Path pout2 = new Path("E:/test/wc/out2");

            // 打包后自定义输入输出路径
//        Path pin2 = new Path(args[1]);
//        Path pout2 = new Path(args[2]);

            // 设置输入路径和输出路径
            FileInputFormat.addInputPath(job2, pin2);
            FileOutputFormat.setOutputPath(job2, pout2);

            // 判断输出路径是否已经存在，若存在则删除
            if (fs.exists(pout2)) {
                fs.delete(pout2, true);
            }

            // 执行 job2
            job2.waitForCompletion(true);
        }
    }
}
