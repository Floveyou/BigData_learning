package hadoop.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * 测试压缩 && 解压缩
 */
public class TestCodec {

    public static void main(String[] args) {
        // SnappyCodec.class 需要配置 Hadoop，然后进行相关操作
        Class[] clazzes = {
                DeflateCodec.class,
                GzipCodec.class,
                BZip2Codec.class,
                Lz4Codec.class,
                LzopCodec.class,
                SnappyCodec.class
        };

        for (Class clazz : clazzes) {
            // 调用压缩方法
            testCompress(clazz);
            // 调用解压缩方法
            testDecompress(clazz);
        }
    }

    /**
     * 测试压缩
     */
    public static void testCompress(Class clazz) {

        try {
            // 获得当前时间
            long start = System.currentTimeMillis();

            Configuration conf = new Configuration();

            // 通过反射获取 CompressionCodec 对象
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(clazz, conf);

            // 获得文件扩展名
            String ext = codec.getDefaultExtension();

            // 通过 codec 获取输出流，将文件进行压缩
            CompressionOutputStream cos = codec.createOutputStream(new FileOutputStream("E:/wc/codec/sdata.txt" + ext));

            // 获取输入流
            FileInputStream fis = new FileInputStream("E:/wc/codec/sdata.txt");

            IOUtils.copyBytes(fis, cos, 1024);

            fis.close();
            cos.close();

            // 计算总时长
            System.out.print("压缩编解码器: " + ext + "压缩时间" + (System.currentTimeMillis() - start));

            File f = new File("E:/wc/codec/sdata.txt" + ext);
            System.out.println("       文件大小: " + f.length());

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    /**
     * 测试解压缩
     *
     * @param clazz
     */
    public static void testDecompress(Class clazz) {
        try {
            // 获得当前时间
            long start = System.currentTimeMillis();

            Configuration conf = new Configuration();

            // 通过反射获取 CompressionCodec 对象
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(clazz, conf);

            // 获得文件扩展名
            String ext = codec.getDefaultExtension();

            // 通过 codec 获取输入流，将文件进行解压缩
            CompressionInputStream cis = codec.createInputStream(new FileInputStream("E:/wc/codec/sdata.txt" + ext));

            // 获取输出流
            FileOutputStream fos = new FileOutputStream("E:/wc/codec/sdata2.txt");

            IOUtils.copyBytes(cis, fos, 1024);

            IOUtils.closeStream(fos);

            cis.close();

            // 计算总时长
            System.out.print("解压缩时间" + (System.currentTimeMillis() - start));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
