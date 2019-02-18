package com.share.kafka.test2;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;

/**
 * @user: share
 * @date: 2019/2/18
 * @description:发送文件
 */
public class TestSendFile {
    @Test
    public void test1() throws Exception {

        FileInputStream fis = new FileInputStream("D:/wc/duowan_user.txt");

        FileOutputStream fos = new FileOutputStream("D:/wc/duowan_user2.txt");

        long start = System.currentTimeMillis();
        int len = 0;
        byte[] buf = new byte[1024];

        while ((len = fis.read(buf)) != -1) {
            fos.write(buf, 0, len);
        }
        System.out.println(System.currentTimeMillis() - start);
        fis.close();
        fos.close();
    }

    //sendFile
    @Test
    public void test2() throws Exception {
        File f = new File("D:/wc/duowan_user4.txt");
        FileInputStream fis = new FileInputStream(f);
        FileOutputStream fos = new FileOutputStream("D:/wc/duowan_user5.txt");

        FileChannel fc1 = fis.getChannel();
        FileChannel fc2 = fos.getChannel();

        long start = System.currentTimeMillis();
        fc1.transferTo(0, f.length(), fc2);
        System.out.println(System.currentTimeMillis() - start);
        fc1.close();
        fc2.close();
        fis.close();
        fos.close();
    }
}
