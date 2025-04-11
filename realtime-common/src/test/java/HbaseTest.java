import constant.Constant;
import lombok.val;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.spark.SparkConf;
import org.junit.Test;
import util.HbaseUtil;

import java.io.IOException;

/**
 * @author Yan
 * @create 2025-04-10 13:22
 **/
public class HbaseTest {
    @Test
    public void test1() {
        Connection conn = HbaseUtil.getConnection();
        System.out.println("获取到链接：" + conn);
        try {
//            HbaseUtil.createTable(conn, Constant.HBASE_NAMESPACE, "dim_product_info", new String[]{"info"});
            HbaseUtil.dropTable(conn, Constant.HBASE_NAMESPACE, "dim_product_info");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    @Test
    public void test2() {
        SparkConf test = new SparkConf()
                .setAppName("test")
                .setMaster("local[2]");

    }
}
