package constant;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Yan
 * @create 2025-04-04 14:58
 **/
public class Constant {

    public static final String TOPIC_DB = "marketing_db";
    public static final String TOPIC_LOG = "marketing_log";
    public static final String PROCESS_DATABASE = "marketing";
    public static final String PROCESS_TABLE_DIM_NAME = "gmall2025_config.table_process_dim";

    public static final String MYSQL_HOST = "hadoop102";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";

    public static final String MYSQL_PASSWORD = "Ybs123123.";
    public static final String HBASE_NAMESPACE = "marketing";
    public static final String HBASE_ZOOKEEPER_QUORUM = "hadoop102, hadoop103, hadoop104";


    public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop102:3306?useSSL=false";
    public static final String NAMESPACE = "marketing";

    public static final String MARKETING_CAMPAIGN_DEFINITION = "dim_marketing_campaign_definition";
    public static final String CUSTOMER_LABEL = "dim_customer_label";

    public static final String TOPIC_DWD_TRIGGER_EVENT = "dwd_trigger_event";
}
