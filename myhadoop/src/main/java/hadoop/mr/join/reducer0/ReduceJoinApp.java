package hadoop.mr.join.reducer0;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @user: share
 * @date: 2018/9/21
 * @description:
 */
public class ReduceJoinApp {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "file:///");

        Job job = Job.getInstance(conf);

        FileSystem fs = FileSystem.get(conf);

        job.setJobName("Reduce Join");

        job.setJarByClass(ReduceJoinApp.class);

        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setGroupingComparatorClass(GroupComparator.class);

        job.setMapOutputKeyClass(CompKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Path inPath = new Path("F:/Archives/OB/Xu/x_hadoop/code2");
        Path outPath = new Path("F:/Archives/OB/Xu/x_hadoop/out/");

        FileInputFormat.addInputPath(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        if(fs.exists(outPath)){
            fs.delete(outPath,true);
        }

//        job.setNumReduceTasks(2);

        job.waitForCompletion(true);




    }
}
