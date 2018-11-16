package hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * IDEA 下测试 HDFS 的增删改查
 */
public class TestHDFS {

    // 1. 测试读取
    @Test
    public void testRead() throws IOException {

        // 初始化配置文件
        Configuration conf = new Configuration();

        // 初始化文件系统
        FileSystem fs = FileSystem.get(conf);

        // 初始化路径
        Path p = new Path("/a.txt");

        // 通过文件系统获取输入流
        // FSDataInputStream 是 inputStream 的装饰流，可以通过普通流方式操纵 fis
        FSDataInputStream fis = fs.open(p);

        int len = 0;
        byte[] buf = new byte[1024];

        while ((len = fis.read(buf)) != -1) {
            System.out.println(new String(buf, 0, len));
        }
        fis.close();
    }

    // 2. 测试读取并通过 IOUtils 拷贝文件到本地
    @Test
    public void testRead2() throws Exception {

        // 初始化配置文件
        Configuration conf = new Configuration();
        // 初始化文件系统
        FileSystem fs = FileSystem.get(conf);

        // 初始化路径
        Path p = new Path("/a.txt");

        // 通过文件系统获取输入流
        // FSDataInputStream 是 inputStream 的装饰流，可以通过普通流方式操纵 fis
        FSDataInputStream fis = fs.open(p);

        FileOutputStream fos = new FileOutputStream("D:/1.txt");

        // 通过 IOUtils 拷贝文件
        IOUtils.copyBytes(fis, fos, 1024);

        fis.close();
        fos.close();
        System.out.println("ok");

    }

    // 3. 测试写文件,将本地文件写入到 HDFS 中
    @Test
    public void testwrite() throws IOException {

        // 设置系统用户名
        System.setProperty("HADOOP_USER_NAME", "centos");

        // 初始化配置文件
        Configuration conf = new Configuration();
        // 初始化文件系统
        FileSystem fs = FileSystem.get(conf);

        // 获得输入流
        FileInputStream fis = new FileInputStream("E:/p_data/wc/customers.txt");

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

    // 创建文件夹
    @Test
    public void testMkdir() throws IOException {

        // 设置系统用户名
        System.setProperty("HADOOP_USER_NAME", "centos");

        // 初始化配置文件
        Configuration conf = new Configuration();
        // 初始化文件系统
        FileSystem fs = FileSystem.get(conf);

        boolean b = fs.mkdirs(new Path("/aaa"));

        System.out.println(b);


    }

    // 删除文件夹
    @Test
    public void testDelete() throws IOException {

        // 设置系统用户名
        System.setProperty("HADOOP_USER_NAME", "centos");

        // 初始化配置文件
        Configuration conf = new Configuration();
        // 初始化文件系统
        FileSystem fs = FileSystem.get(conf);

        boolean b = fs.delete(new Path("/aaa"),true);

        System.out.println(b);
    }

    // 文件末尾追加文件
    @Test
    public void testAppend() throws IOException {

        // 设置系统用户名
        System.setProperty("HADOOP_USER_NAME", "centos");

        // 初始化配置文件
        Configuration conf = new Configuration();
        // 初始化文件系统
        FileSystem fs = FileSystem.get(conf);

        // 通过文件系统获取输出流
        // FSDataOutputStream 是 outputStream 的装饰流，可以通过普通流方式操纵 fos
        FSDataOutputStream fos = fs.append(new Path("/a.txt"));

        // 通过文件系统获取输入流
        // FSDataInputStream 是 inputStream 的装饰流，可以通过普通流方式操纵 fis
        FileInputStream fis = new FileInputStream("E:/p_data/add.txt");

        // 通过 IOUtils 拷贝文件
        IOUtils.copyBytes(fis,fos,1024);

        fis.close();
        fos.close();
    }

    // 通过递归列出指定文件夹下的文件或文件夹信息
    public static void testList(String path) {
        try {
            // 初始化配置文件
            Configuration conf = new Configuration();
            // 初始化文件系统
            FileSystem fs = FileSystem.get(conf);

            FileStatus[] statuses = fs.listStatus(new Path(path));

            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    path = status.getPath().toString();
                    System.out.println(path);
                    testList(path);
                } else {
                    System.out.println(status.getPath().toString());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        testList("/");
    }

}
