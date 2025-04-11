package util

import java.util.ResourceBundle

/**
 * @author Yan
 * @create 2025-04-04 14:27
 * */
object PropertiesUtil {
    private val bundle: ResourceBundle =
        ResourceBundle.getBundle("config")
    
    def apply(key: String): String = {
        bundle.getString(key)
    }
    
    def main(args: Array[String]): Unit = {
        println(PropertiesUtil("kafka.bootstrap.servers"))
    }
}
class PropertiesUtil{

}

