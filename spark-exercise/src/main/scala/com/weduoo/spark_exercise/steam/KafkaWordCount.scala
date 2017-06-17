package com.weduoo.spark_exercise.steam

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.HashPartitioner
import com.weduoo.spark_exercise.LoggerLevels

object KafkaWordCount {
  
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
    //设置参数
    val Array(zkQuorum, qroup, topics, numThreads)= Array[String](
      "192.168.11.101:2181,192.168.11.102:2181,192.168.11.103:2181",
      "g1",
      "lines",
      "2"
    )
    
    val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //LoggerLevels.setStreamingLogLevels()  
    val sc = ssc.sparkContext
    sc.setLogLevel("WARN")
    
    ssc.checkpoint("/Users/weduoo/test/spark/ck02")
    
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val data: ReceiverInputDStream[(String, String)] = 
      KafkaUtils.createStream(ssc, zkQuorum, qroup, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    
      //从kafka中读取的数据key是kafka的key，value是我们写入的数据
    val words = data.map(_._2).flatMap(_.split(" "))
    val partition: HashPartitioner = new HashPartitioner(ssc.sparkContext.defaultMinPartitions)
    val count = words.map((_,1)).updateStateByKey(updateFunc,partition,true)
      
    count.print()
    ssc.start()
    ssc.awaitTermination()
    
  }
}
