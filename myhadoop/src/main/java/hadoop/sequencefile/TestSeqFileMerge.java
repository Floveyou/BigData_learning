package hadoop.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * 测试文件合并，必须是同一种压缩类型
 */
public class TestSeqFileMerge {
    /**
     * 测试序列文件写操作
     * 创建两个文件，范围为1-100，100-200
     */
    @Test
    public void testWriteSeq() throws Exception {

        Configuration conf = new Configuration();

        // 设置文件系统为本地模式
        conf.set("fs.defaultFS", "file:///");

        FileSystem fs = FileSystem.get(conf);

//        Path path = new Path("E:/wc/block1.seq");
        Path path = new Path("E:/wc/block2.seq");

        // 块压缩
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class, SequenceFile.CompressionType.BLOCK);

//        for (int i = 1; i <= 100; i++) {
        for (int i = 101; i <= 200; i++) {
            IntWritable key = new IntWritable(i);
            Text value = new Text("helloworld" + i);

            writer.append(key, value);

        }

        writer.close();
    }

    /**
     * 测试文件合并，合并的同时排序
     */
    @Test
    public void testMerge() throws Exception {
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "file:///");

        FileSystem fs = FileSystem.get(conf);

        Path pin1 = new Path("E:/wc/block1.seq");
        Path pin2 = new Path("E:/wc/block2.seq");
        Path pout = new Path("E:/wc/merge.seq");

        SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs, IntWritable.class, Text.class, conf);

        Path[] p = {pin1, pin2};

        sorter.merge(p, pout);
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

        Path path = new Path("E:/wc/merge.seq");

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
