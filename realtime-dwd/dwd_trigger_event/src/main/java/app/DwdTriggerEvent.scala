package app

import base.BaseSQL
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.streaming.Seconds
import constant.Constant
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.shaded.com.nimbusds.jose.util.StandardCharset
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, from_json, struct, to_json, to_timestamp}
import util.{KafkaUtil, PropertiesUtil}

import java.time.LocalDate

/**
 * @author Yan
 * @create 2025-04-23 16:19
 * */
object DwdTriggerEvent extends BaseSQL {
    def main(args: Array[String]): Unit = {
        DwdTriggerEvent.start(DwdTriggerEvent.getClass.getSimpleName, "local[4]", "dwd_trigger_event",
            Constant.TOPIC_LOG, Seconds(3))
    }
    
    
    override def handle(spark: SparkSession, conf: Configuration, topic: String, groupId: String): Unit = {
        
        // TODO
        
        // 1.创建CustomerInfo表得到id customer_id, customer_number
        createCustomerInfo(spark, conf)
        
        // 2.创建customer_label表得到customer_id, customer_label_id
        createCustomerLabel(spark, conf)
        
        // 3.创建base_dic表得到dic_code,dic_name
        createBaseDic(spark, conf)
        
        // 4.读取主流数据 创建real_time_location
        createRealTimeLocation(spark, topic, groupId)
        
        // 5.window开窗，轻度聚合+维度外键（维度退化） 1min内 基站变化的数据
        val stChangedStream: DataFrame = createJoinedStream(spark)
        
        // 6.写回Kafka
        sinkToKafka(stChangedStream)
        
    }
    
    
    private def sinkToKafka(df: DataFrame): Unit = {
        val query = df
                .select(to_json(struct("*")).as("value"))
                .writeStream
                .outputMode("update")
                .format("kafka")
                .option("kafka.bootstrap.servers", PropertiesUtil("kafka.bootstrap.servers"))
                .option("topic", Constant.TOPIC_DWD_TRIGGER_EVENT)
                .option("checkpointLocation", "hdfs://hadoop102:8020/marketing/stream_" + LocalDate.now().toString + "/")
                .start()
        
        query.awaitTermination()
    }
    
    private def createJoinedStream(spark: SparkSession): DataFrame = {
        spark.sql("select \n" +
                "      dci.rowkey customer_id, \n" +
                "      rtl.customerNumber customer_number, \n" +
                "      dcl.customer_label_ids customer_label_ids, \n" +
                "      attributes, \n" +
                "      realTimeEventId real_time_event_id, \n" +
                "      max(timestamp(from_unixtime(ts/1000, 'yyyy-MM-dd HH:mm:ss'))) event_time, \n" +
                "      max(date_format(from_unixtime(ts/1000, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd')) date_id,\n" +
                "      collect_list(baseStationType) base_station_types, \n" +
                "      collect_list(baseStationId) base_station_ids, \n" +
                "      collect_list(dic_name) base_station_names\n" +
                "from real_time_location rtl \n" +
                "inner join dim_customer_info dci \n" +
                "on rtl.customerNumber = dci.customer_number \n" +
                "inner join dim_customer_label dcl \n" +
                "on dci.rowkey = dcl.customer_id \n" +
                "inner join dim_base_dic dbd \n" +
                "on rtl.baseStationType = dbd.rowkey\n" +
                "group by dci.rowkey, dcl.customer_label_ids, attributes, realTimeEventId, rtl.customerNumber, window(eventTime, '60 SECONDS')" +
                "having count(*) > 1")
    }
    
    private def createBaseDic(spark: SparkSession, conf: Configuration): DataFrame = {
        
        // 1.配置Hbase表
        // 要加载的表和列
        conf.set(TableInputFormat.INPUT_TABLE, Constant.NAMESPACE + ":" + Constant.BASE_DIC)
        conf.set(TableInputFormat.SCAN_COLUMNS, "info:dic_name")
        
        // 2.通过 Hadoop RDD 读取数据
        val hbaseRDD = fromHadoopRDD(spark, conf)
        val rdd: RDD[Row] = hbaseRDD.map(rdd => {
            val result: Result = rdd._2
            val rowkey: String = Bytes.toString(result.getRow)
            val dicName: String =
                Bytes.toString(result.getValue("info".getBytes(StandardCharset.UTF_8), "dic_name".getBytes(StandardCharset.UTF_8)))
            Row(rowkey, dicName)
        })
        val schema: StructType = StructType(Seq(
            StructField("rowkey", StringType, true),
            StructField("dic_name", StringType, true)
        ))
        val df: DataFrame = spark.createDataFrame(rdd, schema)
        df.createOrReplaceTempView(Constant.BASE_DIC)
        df
    }
    
    private def createRealTimeLocation(spark: SparkSession, topic: String, groupId: String): Dataset[RealTimeLocation] = {
        import spark.implicits._
        val df: DataFrame = KafkaUtil.getKafkaDFStream(spark, topic, groupId)
        val jsonSchema: StructType = new StructType()
                .add("RealTimeLocation", new StructType()
                        .add("baseStationId", StringType)
                        .add("baseStationType", StringType)
                        .add("customerNumber", StringType)
                        .add("eventTime", StringType)
                        .add("realTimeEventId", StringType))
                .add("ts", LongType)
        
        val rtlDS: Dataset[RealTimeLocation] = df.selectExpr("CAST(value AS STRING) as json")
                .select(from_json(col("json"), jsonSchema).alias("data"))
                .select(col("data.RealTimeLocation.baseStationId").alias("baseStationId"),
                    col("data.RealTimeLocation.baseStationType").alias("baseStationType"),
                    col("data.RealTimeLocation.customerNumber").alias("customerNumber"),
                    to_timestamp(col("data.RealTimeLocation.eventTime")).alias("eventTime"),
                    col("data.RealTimeLocation.realTimeEventId").alias("realTimeEventId"),
                    col("data.ts"))
                .as[RealTimeLocation]
                .withWatermark("eventTime", "60 SECONDS")
        rtlDS.createOrReplaceTempView("real_time_location")
        rtlDS
    }
    
    private def createCustomerLabel(spark: SparkSession, conf: Configuration): DataFrame = {
        
        // 1.配置Hbase表
        // 要加载的表和列
        conf.set(TableInputFormat.INPUT_TABLE, Constant.NAMESPACE + ":" + Constant.CUSTOMER_LABEL)
        conf.set(TableInputFormat.SCAN_COLUMNS, "info:customer_id info:label_value")
        
        // 2.通过 Hadoop RDD 读取数据
        val hbaseRDD = fromHadoopRDD(spark, conf)
        val rdd: RDD[Row] = hbaseRDD.map(rdd => {
            val result: Result = rdd._2
            val rowkey: String = Bytes.toString(result.getRow)
            val customerId: String = Bytes.toString(result.getValue("info".getBytes(StandardCharset.UTF_8), "customer_id".getBytes(StandardCharset.UTF_8)))
            val labelValue: String = Bytes.toString(result.getValue("info".getBytes(StandardCharset.UTF_8), "label_value".getBytes(StandardCharset.UTF_8)))
            Row(rowkey, customerId, labelValue)
        })
        val schema: StructType = StructType(Seq(
            StructField("rowkey", StringType, true),
            StructField("customer_id", StringType, true),
            StructField("label_value", StringType, true)
        ))
        val df: DataFrame = spark.createDataFrame(rdd, schema)
        df.createOrReplaceTempView("tmp")
        val colSetDf: DataFrame = spark.sql("select\n" +
                "\tcollect_list(rowkey) as customer_label_ids,\n" +
                "\tcustomer_id,\n" +
                "\tcollect_list(label_value) as attributes\n" +
                "from tmp\n" +
                "group by customer_id")
        colSetDf.createOrReplaceTempView(Constant.CUSTOMER_LABEL)
        colSetDf
    }
    
    private def createCustomerInfo(spark: SparkSession, conf: Configuration): DataFrame = {
        
        // 要加载的表和列
        conf.set(TableInputFormat.INPUT_TABLE, Constant.NAMESPACE + ":" + Constant.CUSTOMER_INFO)
        conf.set(TableInputFormat.SCAN_COLUMNS, "info:customer_number")
        
        // 2.通过 Hadoop RDD 读取数据
        val hbaseRDD = fromHadoopRDD(spark, conf)
        val rdd: RDD[Row] = hbaseRDD.map(rdd => {
            val result: Result = rdd._2
            val rowkey: String = Bytes.toString(result.getRow)
            val customerNumber: String = Bytes.toString(result.getValue("info".getBytes(StandardCharset.UTF_8), "customer_number".getBytes(StandardCharset.UTF_8)))
            Row(rowkey, customerNumber)
        })
        val schema: StructType = StructType(Seq(
            StructField("rowkey", StringType, true),
            StructField("customer_number", StringType, true)
        ))
        val df: DataFrame = spark.createDataFrame(rdd, schema)
        df.createOrReplaceTempView(Constant.CUSTOMER_INFO)
        df
    }
    
    private def fromHadoopRDD(spark: SparkSession, conf: Configuration): RDD[(ImmutableBytesWritable, Result)] = {
        spark.sparkContext.newAPIHadoopRDD(
            conf,
            classOf[TableInputFormat],
            classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
            classOf[org.apache.hadoop.hbase.client.Result]
        )
    }
    
}

case class RealTimeLocation(baseStationId: String, baseStationType: String, customerNumber: String,
                            eventTime: String, realTimeEventId: String, ts: Long)
