package com.weduoo.spark_exercise

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager
import java.sql.Date
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object IPLocation {
  
  //数据保存到Mysql操作
  val data2MySQL = (iterator: Iterator[(String, Int)]) => {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "INSERT INTO location_info (location, counts, access_date) VALUES (?, ?, ?)"
    var url = "jdbc:mysql://192.168.11.101:3306/spark"
    try {
      conn = DriverManager.getConnection(url,"root","123456")
      iterator.foreach(line => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, line._1)
        ps.setInt(2, line._2)
        ps.setDate(3, new Date(System.currentTimeMillis()))
        ps.executeUpdate()
      })
    } catch {
      case t: Exception => println("MySQL Exception")
    } finally{
      if(ps != null){
        ps.close()
      }
      if(conn != null){
        conn.close()
      }
    }
  }
  
  //将IP转为long类型，方便进行二分查找
  def ip2Long(ip:String):Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for(i <- 0 until fragments.length){
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }
  
  //二分查找
  def binarySearch(lines: Array[(String, String, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length
    while(low <= high){
      val middle = (low + high) /2
      if((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong)){
        return middle
      }
      if(ip < lines(middle)._1.toLong){
        high = middle - 1
      }else {
        low = middle + 1
      }
    }
    -1
  }
  
  def main(args: Array[String]): Unit = {
    val PATH: String = "/Users/weduoo/test/spark"
    val conf = new SparkConf().setAppName("IPLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    
    val ipRules = sc.textFile(PATH + "/broadcast/ip.txt").map(line =>{
       val fields = line.split("\\|")
       val start_num = fields(2)
       val end_num = fields(3)
       val province = fields(6)
       (start_num, end_num, province)
    })
    
    //将计算好的IP数据搜集回来
    val ipRulesArray = ipRules.collect()
    //广播规则
    val ipRulesBro = sc.broadcast(ipRulesArray)
    
    //加载日志数据
    val ipsRDD = sc.textFile(PATH+"/broadcast/access.log").map( line => {
      val fields = line.split("\\|")
      fields(1)
    })
    val result = ipsRDD.map(ip => {
      val ipNum = ip2Long(ip)
      //获取官广播出去的数据
      val index = binarySearch(ipRulesBro.value, ipNum)
      val info = ipRulesBro.value(index)
      //(ip的起始Num， ip的结束Num，省份名)
      info
    }).map(t => (t._3, 1)).reduceByKey(_+_)
    
    println(result.collect().toBuffer)
    
    result.foreachPartition(data2MySQL(_))
    
    sc.stop()
    
  }
}