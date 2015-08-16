import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by kingcobra on 2015/8/16.
 */
class DataToHbase {
  def readFile{
    val sparkConf = new SparkConf().setMaster("local").setAppName("datatohbase")
    val sc = new SparkContext(sparkConf)
    val dataFile = sc.textFile("file:///e:/testData.txt")
    val pairRDD = dataFile.map(line=>{
      val _a = line.split(",")
      val rowkey = _a(0)+"_"+_a(3)
      (rowkey,line)
    })
   val test= pairRDD.take(10)
    test.foreach(s=>println(s._1+","+s._2))
  }
}
object DataToHbase{
  def main(args: Array[String]) {
    val dh = new DataToHbase
    dh.readFile
  }
}
