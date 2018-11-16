package hadoop.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

/**
 * sequencefile 配置压缩编解码器
 */
public class TestSeqFileCompression {
    /**
     * 测试 sequencefile 配置压缩编解码器进行压缩
     */
    @Test
    public void testWriteSeq() throws Exception {

        Configuration conf = new Configuration();

        // 设置文件系统为本地模式
        conf.set("fs.defaultFS", "file:///");

        FileSystem fs = FileSystem.get(conf);

        // 通过反射获取 CompressionCodec 对象
        // BZip2Codec.class  /  Lz4Codec.class
//        BZip2Codec codec = ReflectionUtils.newInstance(BZip2Codec.class, conf);
        Lz4Codec codec = ReflectionUtils.newInstance(Lz4Codec.class, conf);

//        Path path = new Path("E:/wc/bz2.seq");
        Path path = new Path("E:/wc/lz4.seq");

        // 块压缩
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class, SequenceFile.CompressionType.BLOCK, codec);


        for (int i = 1; i <= 1000000; i++) {
            IntWritable key = new IntWritable(i);
            Text value = new Text("helloworld" + i);

            writer.append(key, value);

        }

        writer.close();
    }


}
