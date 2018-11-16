package hadoop.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * 测试序列文件
 */
public class TestSeqFile {

    /**
     * 测试序列文件写操作
     */
    @Test
    public void testWriteSeq() throws Exception {

        Configuration conf = new Configuration();

        // 设置文件系统为本地模式
        conf.set("fs.defaultFS", "file:///");

        FileSystem fs = FileSystem.get(conf);

//        Path path = new Path("E:/wc/none.seq");
//        Path path = new Path("E:/wc/record.seq");
        Path path = new Path("E:/wc/block.seq");
        // 不压缩
//        sequencefile.Writer writer = sequencefile.createWriter(fs, conf, path, IntWritable.class, Text.class,sequencefile.CompressionType.NONE);
        // 记录压缩
//        sequencefile.Writer writer = sequencefile.createWriter(fs, conf, path, IntWritable.class, Text.class,sequencefile.CompressionType.RECORD);
        // 块压缩
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class, SequenceFile.CompressionType.BLOCK);


        for (int i = 1; i <= 1000; i++) {
            IntWritable key = new IntWritable(i);
            Text value = new Text("helloworld" + i);

            writer.append(key, value);

        }

        writer.close();
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

        Path path = new Path("E:/wc/block.seq");

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
