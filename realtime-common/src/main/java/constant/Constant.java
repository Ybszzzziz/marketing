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

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";

    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel";

    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";

    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success";

    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";
}
