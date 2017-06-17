package com.weduoo.spark_exercise.topn

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.net.URL

/**
 * 使用scala语言内部排序
 */
object UrlTopTypeOne {
  def main(args: Array[String]): Unit = {
    val PATH: String = "/Users/weduoo/test/spark"
    //创建sparkconf，并使用本地两个线程
    val conf = new SparkConf().setAppName("UrlTopTypeOne").setMaster("local[2]")
    //创建sparkContext
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //读取hdfs中的数据
    val lines: RDD[String] = sc.textFile(PATH + "/topn")
    //切分 , 为每个URL计数
    val urlByOne = lines.map(line => {
    	val fields = line.split("\t")
    	val url = fields(1)
    	(url, 1)
    })
    
    //对每一个访问的URL聚合(url1, 10)(url2, 8)
    val urlByLocal = urlByOne.reduceByKey(_+_).cache()
    println(urlByLocal.collect().toBuffer)

    //提取出每一个URL的host和访问量，并按照host分组
    val urlGroupByHost = urlByLocal.map(t => {
    	val host = new URL(t._1).getHost
    	(host, t._1, t._2)
    }).groupBy(_._1)
    println(urlGroupByHost.collect().toBuffer)
    
    //排序内部使用了Scala语言的排序，如果数据量特别大，占用的内存会特别多，有内存溢出的隐患
    //对每一个子域名下的value值进行遍历，并按照访问量排序，去除访问量最大的前三个
    val result = urlGroupByHost.mapValues(_.toList.sortBy(_._3).reverse.take(3))
    println(result.collect().toBuffer)
    //result.saveAsTextFile(PATH + "/out/out-top1")
    //释放资源
    sc.stop()
  }
}