package hadoop.mr.userdraw.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.Map;

/**
 * 将 AppTab 读取到 Map 中
 */
public class ReadAppTab {
    static ConfUtil confUtil = new ConfUtil();

    public static Map<String, String> tabMap = new HashMap<String, String>();

    public static Map<String, String> readFile() {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", confUtil.filesystem);

            FileSystem fs = FileSystem.get(conf);

            Path p = new Path(confUtil.appTab);

            FSDataInputStream fis = fs.open(p);

            String line = null;

            while ((line = fis.readLine()) != null) {
                String[] arr = line.split(confUtil.separator);
                tabMap.put(arr[0], line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tabMap;
    }
}
