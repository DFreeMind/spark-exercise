package com.weduoo.spark_exercise.steam

import org.apache.spark.SparkConf
import com.weduoo.spark_exercise.LoggerLevels
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.dstream.DStream

object SocketUpdateWordCount {
  
  /**
   * Iterator[(String, Seq[Int],Option[Int])]
   * String：表示传入的word，即key值
   * Seq[Int]：表示当前DStream计算批次中，分组聚合之后的value集合，如："weduoo" ->（1,2,4,5）
   * Option[Int]：表示初始值，或者累积之后的历史值
   */
  val updateFunc = (it: Iterator[(String, Seq[Int],Option[Int])]) => {
    it.map{case (x, y, z) => (x, y.sum + z.getOrElse(0))}
  }
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local[2]")
    
    val ssc = new StreamingContext(conf,Seconds(5))
    val sc = ssc.sparkContext
    sc.setLogLevel("WARN")
    
    ssc.checkpoint("/Users/weduoo/test/spark/ck01")
    
    val lines = ssc.socketTextStream("192.168.11.101", 9487, StorageLevel.MEMORY_AND_DISK)
    val words = lines.flatMap(_.split(" "))
    val pairs: DStream[(String, Int)] = words.map((_,1))
    val count = pairs.updateStateByKey(updateFunc,
        new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
    
    count.print()
    ssc.start()
    ssc.awaitTermination()
  }
  
}