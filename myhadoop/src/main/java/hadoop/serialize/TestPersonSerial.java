package hadoop.serialize;

import org.junit.Test;

import java.io.*;

/**
 * 测试 Person 的序列化与反序列化
 */
public class TestPersonSerial {
    /**
     * 单元测试 Person 的序列化
     */
    @Test
    public void testPersonSerial() throws IOException {
        // 新建 Person 对象
        Person p = new Person("sam", 20);
        // 创建 PersonWriteable 对象
        PersonWriteable pw = new PersonWriteable();
        // 调用 set 方法赋值
        pw.setPerson(p);
        // 创建输出流对象
        DataOutputStream dos = new DataOutputStream(new FileOutputStream("E:/person.j"));
        // pw 将值写入输出流 dos
        pw.write(dos);
        // 关闭输出流
        dos.close();
    }

    /**
     * 单元测试Person的反序列化
     * @throws IOException
     */
    @Test
    public void testPersonDeserial() throws IOException {
        // 创建 PersonWriteable 对象
        PersonWriteable pw = new PersonWriteable();
        // 创建输出流对象
        DataInputStream dis = new DataInputStream(new FileInputStream("E:/person.j"));
        // 读取输入流中的对象
        pw.readFields(dis);
        // 得到 Person 对象
        Person p = pw.getPerson();
        // 输出 Person
        System.out.println(p.toString());
        // 关闭输入流
        dis.close();
    }
}
