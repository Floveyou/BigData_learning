package hadoop.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * 测试 MapFile 操作
 * Windows 下查看压缩后的 MapFile :
 * hdfs dfs -text file:///E:/wc/mapfile/index
 * hdfs dfs -text file:///E:/wc/mapfile/data
 */
public class TestMapFile {

    /**
     * 测试序列文件写操作
     */
    @Test
    public void testWriteSeq() throws Exception {

        Configuration conf = new Configuration();

        // 设置文件系统为本地模式
        conf.set("fs.defaultFS", "file:///");

        FileSystem fs = FileSystem.get(conf);

        // 数据存放的文件夹路径
        String path = "E:/wc/mapfile";

        MapFile.Writer writer = new MapFile.Writer(conf, fs, path, IntWritable.class, Text.class);

        for (int i = 1; i <= 100; i++) {
            IntWritable key = new IntWritable(i);
            Text value = new Text("helloworld" + i);
            writer.append(key, value);
        }

        writer.close();
    }

    /**
     * sequencefile 转换为 MapFile
     * 新建文件夹 E:/wc/mapfile2
     * 将 sequencefile 放入其中并重命名为 data
     */
    @Test
    public void SeqConvert() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "centos");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path("file:///E:/wc/mapfile2");
        long cnt = MapFile.fix(fs, p, IntWritable.class, Text.class, false, conf);
        System.out.println(cnt);
    }

}
