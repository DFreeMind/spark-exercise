package com.weduoo.spark_exercise.steam

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext
import com.weduoo.spark_exercise.LoggerLevels
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
 * 从socket中读取数据，进行统计
 */
object SocketWordCount {
  def main(args: Array[String]): Unit = {
    
    //创建SparkConf并设置为本地模式运行
    //注意local[2]代表开两个线程。相当于启动两个线程，一个给receiver，一个给computer。
    //如果是在集群中运行，必须要求集群中可用core数大于1
    val conf = new SparkConf().setAppName("SocketWordCount").setMaster("local[2]")
    //设置日志级别
    LoggerLevels.setStreamingLogLevels()
    //设置DStream的处理时间间隔个2秒
    val ssc = new StreamingContext(conf, Seconds(5))
    val sc = ssc.sparkContext
    sc.setLogLevel("WARN")
    //通过网络读取数据，IP为nc客户端所在服务器
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.11.101", 9487)
    //处理输入的单词
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val pairs: DStream[(String, Int)] = words.map(word => (word,1))
    val count: DStream[(String, Int)] = pairs.reduceByKey(_+_)
    
    //输出数据到控制台
    count.print()
    
    //开始计算
    ssc.start()
    //等待停止
    ssc.awaitTermination()
  }
}