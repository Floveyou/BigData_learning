package hadoop.mr.userdraw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * UserDraw 的 App2 程序
 */
public class UserDrawApp2 {
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
        job.setJobName("UserDrawApp2");

        // job 入口函数类
        job.setJarByClass(UserDrawApp2.class);

        // 设置 mapper 类
        job.setMapperClass(UserDrawMapper2.class);

        // 设置 reducer 类
        job.setReducerClass(UserDrawReducer2.class);

        // 设置 combiner 类
//        job.setCombinerClass(UserDrawReducer.class);

        // 设置 map 的输出 K-V 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CompKey.class);

        // 设置 reduce 的输出 K-V 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置 reduce 分区数量
        job.setNumReduceTasks(1);

        // 新建输入输出路径
        Path pin = new Path("E:/file/userdraw/out");
        Path pout = new Path("E:/file/userdraw/out2");

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
//        boolean b =
        job.waitForCompletion(true);
    }
}
