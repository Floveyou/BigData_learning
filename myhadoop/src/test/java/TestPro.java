import hadoop.mr.userdraw.util.ConfUtil;

/**
 * 测试配置文件工具类的有效性
 */
public class TestPro {
    public static void main(String[] args) {
        ConfUtil confUtil = new ConfUtil();
        System.out.println(confUtil.time);
    }
}
