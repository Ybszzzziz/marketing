package app

import base.BaseSQL
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import constant.Constant
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.shaded.com.nimbusds.jose.util.StandardCharset
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import util.PropertiesUtil

/**
 * @author Yan
 * @create 2025-04-23 16:19
 * */
object DwdTriggerEvent extends BaseSQL {
    // dim_customer_label{customer_id, customer_label_id},
    // dim_marketing_campaign_definition{marketing_campaign_id}, 暂时不需要 等SpringBoot弄完再加
    // dim_real_time_event_definition{real_time_event_id} 唯一值DE002，
    def main(args: Array[String]): Unit = {
        DwdTriggerEvent.start(DwdTriggerEvent.getClass.getSimpleName, "local[4]", "dwd_trigger_event",
            Constant.TOPIC_LOG, Seconds(3))
    }
    
    override def handle(spark: SparkSession, conf: Configuration): Unit = {
        // TODO 只需要加载dim_customer_label
        // 1.配置Hbase表
        // 要加载的表和列
        conf.set(TableInputFormat.INPUT_TABLE, Constant.NAMESPACE + ":" + Constant.CUSTOMER_LABEL)
        conf.set(TableInputFormat.SCAN_COLUMNS, "info:customer_id info:customer_label_id")
        
        // 2.创建customer_label表
//        createCustomerLabel(spark, conf)
        
        // 3.读取主流数据 创建real_time_location表
        val df: DataFrame = spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", PropertiesUtil("kafka.bootstrap.servers"))
                .option("subscribe", "marketing_log")
                .option("startingOffsets", "latest") // 从最新偏移量开始
                .load()
        df.show()
        
        
    }
    
    def createCustomerLabel(spark: SparkSession, conf: Configuration): Unit = {
        // 2.通过 Hadoop RDD 读取数据
        val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(
            conf,
            classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result]
        )
        val rdd: RDD[Row] = hbaseRDD.map(rdd => {
            val result: Result = rdd._2
            val rowkey: String = Bytes.toString(result.getRow)
            val customerId: String = Bytes.toString(result.getValue("info".getBytes(StandardCharset.UTF_8), "customer_id".getBytes(StandardCharset.UTF_8)))
            val customerLabelId: String = Bytes.toString(result.getValue("info".getBytes(StandardCharset.UTF_8), "customer_label_id".getBytes(StandardCharset.UTF_8)))
            Row(rowkey, customerId, customerLabelId)
        })
        val schema: StructType = StructType(Seq(
            StructField("rowkey", StringType, true),
            StructField("customer_id", StringType, true),
            StructField("customer_label_id", StringType, true)
        ))
        val df: DataFrame = spark.createDataFrame(rdd, schema)
        df.createOrReplaceTempView(Constant.CUSTOMER_LABEL)
    }
    
}
