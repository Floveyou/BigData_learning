package hadoop.compression;

import com.hadoop.compression.lzo.LzoIndexer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * LZO 文件的预处理，即在使用 LZO 文件之前添加索引
 */
public class TestLzoIndex {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 压缩编解码器必须是此类或其子类
        conf.set("io.compression.codecs","com.hadoop.compression.lzo.LzopCodec");
        LzoIndexer indexer = new LzoIndexer(conf);
        indexer.index(new Path("file:///E:/wc/codec/sdata.txt.lzo"));
    }
}
