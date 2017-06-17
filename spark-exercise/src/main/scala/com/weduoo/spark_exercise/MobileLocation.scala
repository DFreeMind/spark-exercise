package com.weduoo.spark_exercise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object MobileLocation {
  def main(args: Array[String]): Unit = {
    val PATH: String = "/Users/weduoo/test/spark";
    //设置本地模式运行，并使用两个线程
    val conf = new SparkConf().setAppName("MobileLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    
    //读取手机连接基站的信息
    val lines = sc.textFile(PATH + "/mobile_log")
    
   //切分
    val splited = lines.map(line => {
      val fields = line.split(",")
      val mobile = fields(0)
      val location = fields(2)
      val tp = fields(3)
      val time = if(tp == "1") -fields(1).toLong else fields(1).toLong
      //组装数据
      ((mobile, location),time)
    })
    
    //分组聚合
    val reduced:RDD[((String,  String), Long)] = splited.reduceByKey(_+_)
    //数据转换
    val mobile_duration_time = reduced.map(x => {
      //(手机号, 基站),时间) => (基站，（手机号，时间）)
      (x._1._2, (x._1._1, x._2))
    })
    
    //读取基站信息
    val locations: RDD[String] = sc.textFile(PATH + "/mobile_location")
    val localInfo = locations.map(line => {
      val fields = line.split(",")
      val id = fields(0)
      val x = fields(1)
      val y = fields(2)
      //组装数据
      (id,(x,y))
    })
    //将连接时间与基站基本信息表连接
    val mobile_localtion_time: RDD[(String, ((String, Long), (String, String)))] =  
              mobile_duration_time.join(localInfo)
    println(mobile_localtion_time.collect.toBuffer)
    
    //按手机号分组(基站ID, ((手机号, 时间), (经度, 纬度)))
    val groupByMobile = mobile_localtion_time.groupBy(_._2._1._1)
    println(groupByMobile.collect().toBuffer)
    
    //按停留时间排序
    val orderByDuration = groupByMobile.mapValues(_.toList.sortBy(_._2._1._2).reverse.take(2))
    println(orderByDuration.collect().toBuffer)
    
    sc.stop()
  }
  
}