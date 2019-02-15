import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * @user: share
 * @date: 2019/2/15
 * @description:测试在建关注表的同时添加weibo协处理器
 */
public class TestAddCopro2 {
    public static void main(String[] args) throws Exception {
        //初始化hbase 的conf
        Configuration conf = HBaseConfiguration.create();

        //通过连接工厂创建连接
        Connection conn = ConnectionFactory.createConnection(conf);

        //获取hbase管理员
        Admin admin = conn.getAdmin();

        TableName table = TableName.valueOf("weibo:guanzhu");

        HTableDescriptor htd = new HTableDescriptor(table);

        htd.addCoprocessor("com.share.hbase.weibo.Weibo", new Path("/myhbase.jar"), 0, null);

        htd.addFamily(new HColumnDescriptor("f1"));

        admin.createTable(htd);

        admin.close();
        conn.close();

    }
}
