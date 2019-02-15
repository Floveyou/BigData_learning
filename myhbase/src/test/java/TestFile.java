import org.junit.Test;

import java.io.FileOutputStream;

/**
 * @user: share
 * @date: 2019/2/15
 * @description: 测试输出文件
 */
public class TestFile {
    @Test
    public void testCreateFile() {
        try {
            FileOutputStream fos = new FileOutputStream("e:/test/coprocessor.log", true);
            fos.write("hello woeld\n".getBytes());

            fos.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
