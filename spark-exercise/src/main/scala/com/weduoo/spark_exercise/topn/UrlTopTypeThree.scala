package com.weduoo.spark_exercise.topn

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Partitioner
import java.net.URL
import scala.collection._
import com.weduoo.spark_exercise.LoggerLevels

object UrlTopTypeThree {
  def main(args: Array[String]): Unit = {
    val PATH: String = "/Users/weduoo/test/spark"
    LoggerLevels.setStreamingLogLevels()
    //创建sparkconf，并使用本地两个线程
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    //创建sparkContext
    val sc = new SparkContext(conf)
    
    //读取hdfs中的数据
    val lines: RDD[String] = sc.textFile(PATH + "/topn")
    //切分 
    val urlByOne = lines.map(line => {
    	val fields = line.split("\t")
    	val url = fields(1)
    	(url, 1)
    })

    val urlByLocal = urlByOne.reduceByKey(_+_)

    val urlGroupByHost: RDD[(String, (String, Int))] = urlByLocal.map(t => {
    	val host = new URL(t._1).getHost
    	//域名，url，访问次数
    	(host, (t._1, t._2))
    })

    //获取host域名
    val hosts: Array[String] = urlGroupByHost.map(_._1).distinct().collect()
    println(hosts.toBuffer)
    
    val partitioner: HashPartitioner = new HashPartitioner(hosts)
    //按照自定义规则进行分区
    val repartitionedUrl = urlGroupByHost.partitionBy(partitioner)

    //在一个分区上进行排序
    val result = repartitionedUrl.mapPartitions(iter => {
      //(域名, (URL, 访问次数))
    	iter.toList.sortBy(_._2._2).reverse.take(3).iterator
    })
    
    println(result.collect().toBuffer)

    //result.saveAsTextFile("")

    //释放资源
    sc.stop()
  }
}
class HashPartitioner(hosts: Array[String]) extends Partitioner {

  //不同的子域名拥有不同的index值
	val rules = new mutable.HashMap[String, Int]()
	var index = 0
	for( host <- hosts) {
		rules.put(host, index)
		index += 1
	}
	println(rules)
	override def getPartition(key: Any) : Int = {
	  //返回当前子域名的index，找不到则返回默认值
		val host = key.toString
		rules.getOrElse(host, 0)
	}

	override def numPartitions: Int = hosts.length
}