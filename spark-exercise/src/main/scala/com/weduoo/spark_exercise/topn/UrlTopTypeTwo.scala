package com.weduoo.spark_exercise.topn

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object UrlTopTypeTwo {
  def main(args: Array[String]): Unit = {
    val PATH: String = "/Users/weduoo/test/spark"
    
  	val hosts = Array("http://java.weduoo.com","http://php.weduoo.com",
  	      "http://net.weduoo.com")

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
    //对每一个访问的URL聚合(url1, 10)(url2, 8)
    val urlByLocal = urlByOne.reduceByKey(_+_)
    println(urlByLocal.collect().toBuffer)

    //循环过滤
    for(u <- hosts){
    	val filteredUrl = urlByLocal.filter(t => {
    		val url = t._1
    		url.startsWith(u)
    	})
    	val result = filteredUrl.sortBy(_._2, false).take(3)
    	println(result.toBuffer)
    }

    //释放资源
    sc.stop()
  }
}