package com.kingcobra.scala

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf,SparkContext}

/**
 * Created by kingcobra on 2015/8/16.
 */
class DataToHbase {
//  val MASTER ="spark://192.168.1.114:7077"
  val MASTER ="local"
  val HBASETABLENAME="test"
  val CF="cf1"
  val configuration = HBaseConfiguration.create()
  configuration.addResource("core-site.xml")
  val OUTPUTPATH="/user/kingcobra/hfile"
  def readFile{
    val sparkConf = new SparkConf().setMaster(MASTER).setAppName("datatohbase")
    val sc = new SparkContext(sparkConf)
//    val dataFile = sc.textFile("file:///e:/testData.txt")
    val dataFile = sc.textFile("file:////Users/kingcobra/Documents/works/testData.txt")
    val pairRDD = dataFile.map(line=>{
      val _a = line.split(",")
      var rowkey = _a(0)+"_"+_a(3)+"_"+_a(7)
      (rowkey,line)
    }).cache().sortByKey(true)

   val sortedPairRDD= pairRDD.map(x=>{
     var _k=Bytes.toBytes(x._1)
     val rowkey = new ImmutableBytesWritable(_k)
     val keyvalue = new KeyValue(_k,Bytes.toBytes(CF),Bytes.toBytes("info"),Bytes.toBytes(x._2))
     Seq(rowkey,keyvalue)
   })
    val job = Job.getInstance(configuration)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    var hTable = new HTable(configuration,HBASETABLENAME.getBytes())
    HFileOutputFormat.configureIncrementalLoad(job,hTable)
    var filesystem = FileSystem.get(configuration)
    sortedPairRDD.saveAsObjectFile(OUTPUTPATH)
//   test.foreach(s=>println(s._1+","+s._2))
  }
}

object DataToHbase{
  def main(args: Array[String]) {
    val dh = new DataToHbase
    dh.readFile
  }
}
