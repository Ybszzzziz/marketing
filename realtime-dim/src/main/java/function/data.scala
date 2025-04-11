package function

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/**
 * @author Yan
 * @create 2025-04-11 11:00
 * */
object data {
    def etl(KafkaDStream: InputDStream[ConsumerRecord[String, String]]): DStream[ConsumerRecord[String, String]] = {
        KafkaDStream.filter(record => {
            try {
                val jsonString: String = record.value()
                val jSONObject: JSONObject = JSON.parseObject(jsonString)
                val database: AnyRef = jSONObject.get("database")
                val dataType: AnyRef = jSONObject.get("type")
                val data: JSONObject = jSONObject.getJSONObject("data")
                ("marketing".equals(database) || "marketing_config".equals(database)) &&
                        !"bootstrap-start".equals(dataType) &&
                        !"bootstrap-complete".equals(dataType) &&
                        data.size() != 0
            } catch {
                case e: Exception => {
                    e.printStackTrace()
                    false
                }
            }
        })
    }
}
