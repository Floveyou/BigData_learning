package hadoop.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.Random;

/**
 * 测试排序
 */
public class TestSeqFileSort {

    /**
     * 创建无序 key-value 文件
     */
    @Test
    public void testWriteRandom() throws Exception {

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "file:///");

        FileSystem fs = FileSystem.get(conf);

        Path p = new Path("E:/wc/random.seq");

        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, p, IntWritable.class, Text.class, SequenceFile.CompressionType.RECORD);

        // 初始化 random
        Random r = new Random();

        for (int i = 1; i < 100000; i++) {
            // 在0-99999之中随机选取一个值
            int j = r.nextInt(100000);
            IntWritable key = new IntWritable(j);
            Text value = new Text("helloworld" + j);

            writer.append(key, value);

        }

        writer.close();

    }

    /**
     * 测试seqFile排序
     */
    @Test
    public void testSort() throws Exception {

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "file:///");

        FileSystem fs = FileSystem.get(conf);

        Path pin = new Path("E:/wc/random.seq");
        Path pout = new Path("E:/wc/sort.seq");

        SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs, IntWritable.class, Text.class, conf);

        sorter.sort(pin, pout);
    }

    /**
     * 测试序列文件读操作
     */
    @Test
    public void testReadSeq() throws Exception {
        Configuration conf = new Configuration();

        // 设置文件系统为本地模式
        conf.set("fs.defaultFS", "file:///");

        FileSystem fs = FileSystem.get(conf);

        Path path = new Path("E:/wc/sort.seq");

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

        //初始化两个 Writable 对象
        IntWritable key = new IntWritable();
        Text value = new Text();

        while ((reader.next(key, value))) {
            long position = reader.getPosition();
            System.out.println("key: " + key.get() + " , " + " val: " + value.toString() + " , " + " pos: " + position);
        }
    }

}
