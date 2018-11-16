package hadoop.serialize;

import org.junit.Test;

import java.io.*;

/**
 * 测试 Java 序列化 & 反序列化
 */
public class TestJavaSerial {
    /**
     * 测试 Java 序列化
     */
    @Test
    public void testSerial() throws Exception {
        Integer i = 100;

        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("E:/serial.j"));
        oos.writeObject(i);

        oos.close();
    }

    /**
     * 测试 Java 反序列化
     */
    @Test
    public void testDeserial() throws Exception {
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream("E:/serial.j"));

        Object o = ois.readObject();

        System.out.println((Integer) o);
    }
}
