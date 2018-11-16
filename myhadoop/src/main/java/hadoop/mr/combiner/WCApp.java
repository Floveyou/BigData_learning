package hadoop.mr.combiner;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Word Count APP
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
        job.setJobName("Word Count");

        // job 入口函数类
        job.setJarByClass(WCApp.class);

        // 设置 mapper 类
        job.setMapperClass(WCMapper.class);

        // 设置 reducer0 类
        job.setReducerClass(WCReducer.class);

        // 设置 combiner 类
        job.setCombinerClass(hadoop.mr.wc.WCReducer.class);

        // 设置 map 的输出 K-V 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置 reduce 的输出 K-V 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 新建输入路径和输出路径
        Path pin = new Path("E:/test/wc/");
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
        job.waitForCompletion(true);
    }
}
