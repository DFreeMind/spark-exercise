package com.weduoo.spark_exercise.game

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import kafka.serializer.StringDecoder

/**
 * 游戏外挂扫描
 */
object ScannerPlugins {
  def main(args: Array[String]): Unit = {
    val PATH = "/Users/weduoo/test/spark/"
    val Array(zkQuorum, group, topics, numThreads) =
        Array("192.168.11.101:2181,192.168.11.102:2181,192.168.11.103:2181",
        "g0", "weduoo-game-log", "1" )
    
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val conf = new SparkConf().setAppName("ScannerPlugins").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //设置产生时间批次的间隔
    val ssc = new StreamingContext(sc, Milliseconds(10000))
    //设置CheckpointDir
    sc.setCheckpointDir(PATH + "ck-scannerplugins")
    
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group,
      "auto.offset.reset" -> "smallest"
    )
    
    val dstream: ReceiverInputDStream[(String, String)] = 
      KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK)
    //取出kafka中的数据
    val lines = dstream.map(_._2)
    //切分数据
    val splitedLines = lines.map(_.split("    "))
    val filteredLines = splitedLines.filter( f => {
    	println(f.size)
      println(f.toBuffer)
      if(f.size != 1) {
        val et = f(3)
        val item = f(8)
        et == "11" && item == "强效太阳水"
      }else{
        false
      }
    })
    
    val userAndTime = filteredLines.map(f => (f(7), dateFormat.parse(f(12)).getTime))
    //按照窗口时间进行分组
    val groupWindow = userAndTime.groupByKeyAndWindow(Milliseconds(30000), Milliseconds(20000))
    val filtered = groupWindow.filter(_._2.size >= 5)
    
    //计算药物的平均使用时间
    val itemAvgTime = filtered.mapValues(it => {
      val list = it.toList.sorted
      val size = list.size
      val first = list(0)
      val last = list(size - 1)
      val cha = last - first
      cha / size
    })
    
    val badUser = itemAvgTime.filter(_._2 < 10000)
    
    //将计算出来的数据写入redis
    badUser.foreachRDD(rdd => {
      rdd.foreachPartition( it => {
//        val conn = JedisConnectionPool.getConnection()
//        it.foreach(t => {
//          val user = t._1
//          val avgTime = t._2
//          val currenTime = System.currentTimeMillis()
//          conn.set(user+ "_" + currenTime, avgTime.toString())
//        })
//        conn.close()
        it.foreach(t => {
          println(t._1)
        })
      })
    })
    
    ssc.start()
    ssc.awaitTermination()
  }
}