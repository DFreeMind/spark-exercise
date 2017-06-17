package com.weduoo.spark_exercise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object WordCount {
  def main(args: Array[String]): Unit = {
    val PATH: String = "/Users/weduoo/test/spark";
    //创建sparkconf
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    //创建sparkContext
    val sc = new SparkContext(conf)
    
    //读取hdfs中的数据
    val lines: RDD[String] = sc.textFile(PATH + "/wordcount")
    //切分单词
     val words: RDD[String] = lines.flatMap(_.split(" "))
     //计算单词
     val wordCount: RDD[(String, Int)] = words.map((_,1))
     //分组聚合
     val result: RDD[(String, Int)] = wordCount.reduceByKey(_+_).cache()
     //排序得到最总结果
     val sortByRes = result.sortBy(_._2, false)
     println(sortByRes.collect().toBuffer)
     //sortByRes.saveAsTextFile(PATH + "/wordcount-out")
     //释放资源
    sc.stop()
  }
}