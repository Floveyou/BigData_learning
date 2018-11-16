package hadoop.serialize;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * Hadoop 序列化 & 反序列化
 */
public class TestHadoopSerial {

    /**
     * Hadoop 序列化
     */
    @Test
    public void testSerial() throws Exception{
        // 创建IntWritable对象
        IntWritable iw = new IntWritable(100);
        // 创建输出流对象
        DataOutputStream dos = new DataOutputStream(new FileOutputStream("E:/serial.h"));
        // iw将值写入输出流dos
        iw.write(dos);
        // 关闭输出流
        dos.close();
    }

    /**
     * Hadoop 反序列化
     */
    @Test
    public void testDeserial() throws Exception {
        // 创建输入流对象
        DataInputStream dis = new DataInputStream(new FileInputStream("E:/serial.h"));
        // 创建 IntWritable 对象
        IntWritable iw = new IntWritable();
        // iw 读取输入流 dis 的值
        iw.readFields(dis);
        // 得到 iw 中的值
        int i = iw.get();
        // 输出 i
        System.out.println(i);
        // 关闭输入流
        dis.close();
    }
}
