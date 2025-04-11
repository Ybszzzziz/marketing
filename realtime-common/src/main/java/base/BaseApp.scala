package base

import constant.Constant
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import util.KafkaUtil

/**
 * @author Yan
 * @create 2025-04-04 14:46
 * */
trait BaseApp {
    
    def handle(KafkaDStream: InputDStream[ConsumerRecord[String, String]], ssc: StreamingContext, sc: SparkContext): Unit = ???
    
    def start(appName: String, master: String, groupId: String, topic: String, batchDuration: Duration):Unit = {
        
        val conf: SparkConf = new SparkConf()
                .setAppName(appName)
                .setMaster(master)
        val ssc = new StreamingContext(conf, batchDuration)
        val KafkaDStream: InputDStream[ConsumerRecord[String, String]] =
            KafkaUtil.getKafkaDStream(topic, ssc, groupId)
        val sc: SparkContext = ssc.sparkContext
        val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        
        
        handle(KafkaDStream, ssc, sc)
        
        ssc.start()
        ssc.awaitTermination()
    }
    
}
