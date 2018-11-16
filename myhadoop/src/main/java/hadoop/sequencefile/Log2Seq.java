package hadoop.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * 测试将日志文件转换成序列文件
 * Windows 下查看压缩后的 sequencefile :
 * hdfs dfs -text file:///E:/wc/access.seq
 */
public class Log2Seq {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // 设置文件系统为本地模式
        conf.set("fs.defaultFS", "file:///");

        FileSystem fs = FileSystem.get(conf);

        Path path = new Path("E:/wc/access.seq");

        // 不压缩
//        sequencefile.Writer writer = sequencefile.createWriter(fs, conf, path, IntWritable.class, Text.class,sequencefile.CompressionType.NONE);
        // 记录压缩
//        sequencefile.Writer writer = sequencefile.createWriter(fs, conf, path, IntWritable.class, Text.class,sequencefile.CompressionType.RECORD);
        // 块压缩
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, NullWritable.class, Text.class, SequenceFile.CompressionType.BLOCK);

        BufferedReader br = new BufferedReader(new FileReader("E:/file/access.log1"));

        String line = null;
        while ((line = br.readLine()) != null) {
            NullWritable key = NullWritable.get();
            Text value = new Text(line);
            writer.append(key, value);
        }

        writer.close();
    }
}
