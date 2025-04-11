package function

import bean.TableProcessDim
import com.alibaba.fastjson.{JSON, JSONObject}
import constant.Constant
import org.apache.hadoop.hbase.client.Connection
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import util.HbaseUtil

import java.util
import scala.collection.mutable

/**
 * @author Yan
 * @create 2025-04-11 11:00
 * */
object hbase {
    def sinkToHbase(etlStream: DStream[ConsumerRecord[String, String]], broadcastMap: Broadcast[mutable.Map[String, TableProcessDim]]): Unit = {
        etlStream.foreachRDD(rdd => {
            rdd.foreachPartition(partition => {
                val conn: Connection = HbaseUtil.getConnection
                partition.foreach((record: ConsumerRecord[String, String]) => {
                    val jSONObject: JSONObject = getJSONObject(record.value())
                    val table: AnyRef = jSONObject.get("table")
                    val op: AnyRef = jSONObject.get("type")
                    val map: mutable.Map[String, TableProcessDim] = broadcastMap.value
                    // 是维度表
                    if (map.contains(table.toString)) {
                        val data: JSONObject = jSONObject.getJSONObject("data")
                        
                        // 获取对应的配置表
                        val option: Option[TableProcessDim] = map.get(table.toString)
                        val tableProcessDim: TableProcessDim = option.get
                        
                        // 筛选字段
                        data.keySet().removeIf(key => util.Arrays.asList(tableProcessDim.getSinkColumns.split(",")).contains(key))
                        op match {
                            case "delete" => HbaseUtil.deleteCells(conn, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable, data.get(tableProcessDim.getSinkRowKey).toString)
                            case _ => HbaseUtil.putCells(conn, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable,
                                data.get(tableProcessDim.getSinkRowKey).toString, tableProcessDim.getSinkFamily, data)
                        }
                    }
                })
                conn.close()
            })
        })
    }
    
    def maintainHbase(etlStream: DStream[ConsumerRecord[String, String]], broadcastMap: Broadcast[mutable.Map[String, TableProcessDim]]): Unit = {
        etlStream.foreachRDD(rdd => {
            rdd.foreachPartition(partition => {
                val conn: Connection = HbaseUtil.getConnection
                partition.foreach(record => {
                    val jSONObject: JSONObject = getJSONObject(record.value())
                    val map: mutable.Map[String, TableProcessDim] = broadcastMap.value
                    val table: TableProcessDim = jSONObject.getObject[TableProcessDim]("data", classOf[TableProcessDim]: Class[TableProcessDim])
                    val op: AnyRef = jSONObject.get("type")
                    if ("table_process_dim".equals(jSONObject.get("table"))) {
                        op match {
                            case "bootstrap-insert" => createTable(table)
                            case "insert" => createTable(table)
                            case "delete" => deleteTable(table)
                            case "update" => {
                                deleteTable(table)
                                createTable(table)
                            }
                        }
                    }
                    
                    map.put(table.getSourceTable, table)
                    
                    def createTable(table: TableProcessDim): Unit = {
                        HbaseUtil.createTable(conn, Constant.HBASE_NAMESPACE, table.getSinkTable, table.getSinkFamily.split(","))
                    }
                    
                    def deleteTable(table: TableProcessDim): Unit = {
                        HbaseUtil.dropTable(conn, Constant.HBASE_NAMESPACE, table.getSinkTable)
                    }
                })
                conn.close()
            })
        })
    }
    
    private def getJSONObject(jsonString: String): JSONObject = {
        JSON.parseObject(jsonString)
    }
}
