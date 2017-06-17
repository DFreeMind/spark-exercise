package com.weduoo.spark_exercise.steam

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.flume.FlumeUtils
import java.net.InetSocketAddress
import org.apache.spark.storage.StorageLevel

object FlumeWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FlumeWordCount").setMaster("local[2]")
    
    val ssc = new StreamingContext(conf, Seconds(5))
    val sc = ssc.sparkContext
    sc.setLogLevel("WARN")
    
    //从socket中创建RDD
    val ips = Array(new InetSocketAddress("192.168.11.101",9487))
    //通过FlumeUtils创建一个poll类型的连接
    val flume = FlumeUtils.createPollingStream(ssc, ips, StorageLevel.MEMORY_AND_DISK)
    //数据存放在flume的Body中
    val words = flume.flatMap(x => new String(x.event.getBody().array()).split(" "))
    val pairs = words.map((_,1))
    val result = pairs.reduceByKey(_+_)
    
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}