package hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * HDFS 文件写入流程分析
 */
public class TestHDFS2 {

    // 1. 测试读取
    @Test
    public void testRead() throws IOException {

        // 设置系统用户名
        System.setProperty("HADOOP_USER_NAME", "centos");

        // 初始化配置文件
        Configuration conf = new Configuration();
        // 初始化文件系统
        FileSystem fs = FileSystem.get(conf);

        // 获得输入流
        FileInputStream fis = new FileInputStream("E:/file/orders.txt");

        // 初始化路径
        Path pout = new Path("/b.txt");

        // 通过文件系统获取输出流
        // FSDataOutputStream 是 outputStream 的装饰流，可以通过普通流方式操纵 fos
        FSDataOutputStream fos = fs.create(pout);

        // 通过 IOUtils 拷贝文件
        IOUtils.copyBytes(fis, fos, 1024);

        fis.close();
        fos.close();
        System.out.println("ok");

    }
}
