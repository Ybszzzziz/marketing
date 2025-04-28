package base

import constant.Constant
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.shaded.com.nimbusds.jose.util.StandardCharset
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Duration, StreamingContext}
import util.KafkaUtil

/**
 * @author Yan
 * @create 2025-04-23 13:41
 * */
trait BaseSQL {
    
    def start(appName: String, master: String, groupId: String, topic: String, batchDuration: Duration):Unit = {
        
        val sc: SparkConf = new SparkConf()
                .setAppName(appName)
                .setMaster(master)
        val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()
        
        
        val conf: Configuration = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM)
        
        handle(spark, conf, topic, groupId)
        
//        conf.set(TableInputFormat.INPUT_TABLE, "marketing:dim_marketing_campaign_definition")
//        conf.set(TableInputFormat.SCAN_COLUMNS, "info:create_time") // 指定列
//
//        // 通过 Hadoop RDD 读取数据
//        val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(
//            conf,
//            classOf[TableInputFormat],
//            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
//            classOf[org.apache.hadoop.hbase.client.Result]
//        )
//        val rdd: RDD[Row] = hbaseRDD.map(rdd => {
//            val result: Result = rdd._2
//            val rowkey: String = Bytes.toString(result.getRow)
//            val createTime: String = Bytes.toString(result.getValue("info".getBytes(StandardCharset.UTF_8), "create_time".getBytes(StandardCharset.UTF_8)))
//            Row(rowkey, createTime)
//        })
//        val schema: StructType = StructType(Seq(
//            StructField("rowkey", StringType, true),
//            StructField("create_time", StringType, true)
//        ))
//
//        val df: DataFrame = spark.createDataFrame(rdd, schema)
//        df.createOrReplaceTempView("marketing_campaign_definition")
    }
    def handle(spark: SparkSession, conf: Configuration, topic: String, groupId: String)
}
