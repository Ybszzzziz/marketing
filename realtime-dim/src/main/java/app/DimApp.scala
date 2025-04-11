package app

import bean.TableProcessDim
import base.BaseApp
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import constant.Constant
import function.data.etl
import function.hbase.{maintainHbase, sinkToHbase}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import scala.collection.mutable


/**
 * @author Yan
 * @create 2025-04-04 14:35
 * */
object DimApp extends BaseApp {
    def main(args: Array[String]): Unit = {
        DimApp.start(DimApp.getClass.getSimpleName,
            "local[4]", "dim_app", Constant.TOPIC_DB, Seconds(3))
    }
    
    
    override def handle(KafkaDStream: InputDStream[ConsumerRecord[String, String]], ssc: StreamingContext, sc: SparkContext): Unit = {
        //TODO 首次需要bootstrap配置表
        // 1. 过滤数据，保证格式正确，来源正确
        val etlStream: DStream[ConsumerRecord[String, String]] = etl(KafkaDStream)
        val map: mutable.Map[String, TableProcessDim] = mutable.Map()
        val broadcastMap: Broadcast[mutable.Map[String, TableProcessDim]] = sc.broadcast(map)
        
        // 2. 创建Hbase维度表(动态维护维度表)
        maintainHbase(etlStream, broadcastMap)
        
        // 3.写入Hbase
        sinkToHbase(etlStream, broadcastMap)
        
        
    }
    
    
    
    
    
    
}
