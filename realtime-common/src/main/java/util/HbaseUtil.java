package util;

import constant.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Yan
 * @create 2025-03-12 16:35
 **/
public class HbaseUtil {
    private static List<Put> puts = new ArrayList<>();

    /**
     * 获取连接
     *
     * @return hbase连接对象
     */
    public static Connection getConnection() {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM);
        conf.setInt("hbase.zookeeper.property.timeout", 60000);
        conf.setInt("zookeeper.recovery.retry.intervalmill", 1000); // 重试间隔
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return conn;

    }

    /**
     * 关闭连接
     *
     * @param conn hbase连接对象
     */
    public static void closeConnection(Connection conn) {
        try {
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表格
     *
     * @param conn      Hbase同步连接
     * @param namespace 命名空间
     * @param table     表名
     * @param families  列族名
     * @throws IOException
     */
    public static void createTable(Connection conn, String namespace, String table, String[] families) throws IOException {
        if (families == null || families.length == 0) {
            System.out.println("创建habse table 至少需要1哥列祖");
            return;
        }
        Admin admin = conn.getAdmin();

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, table));
        for (String family : families) {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family))
                    .build();
            tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        }
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            System.out.println("table already exist!");
        }
        admin.close();
    }

    /**
     * 删除表格
     *
     * @param conn      Hbase同步连接
     * @param namespace 命名空间
     * @param table     表名
     * @throws IOException
     */
    public static void dropTable(Connection conn, String namespace, String table) throws IOException {
        Admin admin = conn.getAdmin();
        try {
            admin.disableTable(TableName.valueOf(namespace, table));
            admin.deleteTable(TableName.valueOf(namespace, table));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 写到Hbase
     *
     * @param conn      Hbase同步连接
     * @param namespace 命名空间
     * @param tableName 表名
     * @param rowKey    主键
     * @param family    列族名
     * @param data      JSON:{列名:列值}
     */
    public static void putCells(Connection conn, String namespace, String tableName, String rowKey,
                                String family, JSONObject data) throws IOException {
        // 获取table
        Table table = conn.getTable(TableName.valueOf(namespace, tableName));

        // 创建写入对象
        Put put = new Put(Bytes.toBytes(rowKey));
        for (String col : data.keySet()) {
            String colVal = data.getString(col);
            if (colVal != null) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(col), Bytes.toBytes(colVal));
            }
        }
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 关闭table
        table.close();
    }

    public static Put getPut(Connection conn, String namespace, String tableName, String rowKey,
                                String family, JSONObject data) throws IOException {

        // 创建写入对象
        Put put = new Put(Bytes.toBytes(rowKey));
        for (String col : data.keySet()) {
            String colVal = data.getString(col);
            if (colVal != null) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(col), Bytes.toBytes(colVal));
            }
        }
        return put;
    }


    /**
     * 删除一整行
     *
     * @param conn      Hbase同步连接
     * @param namespace 命名空间
     * @param tableName 表名
     * @param rowKey    列族名
     * @throws IOException
     */
    public static void deleteCells(Connection conn, String namespace, String tableName, String rowKey) throws IOException {
        // 获取table
        Table table = conn.getTable(TableName.valueOf(namespace, tableName));

        Delete delete = new Delete(Bytes.toBytes(rowKey));
        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        table.close();
    }

}
