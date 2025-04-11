import util.HbaseUtil

object test {
    def main(args: Array[String]): Unit = {
        println(HbaseUtil.getConnection)
    }
}