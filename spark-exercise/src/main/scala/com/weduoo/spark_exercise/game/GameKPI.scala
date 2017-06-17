package com.weduoo.spark_exercise.game

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 游戏日志中相关指数的计算
 * @author weduoo
 * @ontime 2017年06月16日11:37:16
 */
object GameKPI {
  def main(args: Array[String]): Unit = {
    val PATH = "/Users/weduoo/test/spark/"
    
    val queryTime = "2016-02-01 00:00:00"
    val beginTime = TimeUtils(queryTime)
    
    val endTime = TimeUtils.getCertainDayTime(+1)
    
    val time1 = TimeUtils.getCertainDayTime(+1)
    val time2 = TimeUtils.getCertainDayTime(+2)
    
    val conf = new SparkConf().setAppName("GameKPI").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    
    //数据格式：1|2016年2月1日,星期一,10:01:08|10.51.4.168|李明克星|法师|男|1|0|0/800000000
    val splitedLog = sc.textFile(PATH + "game/GameLog.txt").map(_.split("\\|"))
    
    //    splitedLog.filter(_(0) == "1").filter(x => {
    //      val time = x(1)
    //      //不好，每filter一次就会new一个SimpleDateFormat实例，会浪费资源
    //      val sdf = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")
    //      val timeLong = sdf.parse(time).getTime
    //    })
    
    //过滤并缓存数据，供以后计算使用
    val filterLogs = splitedLog
      .filter(fields => FilterUtils.filterByTime(fields, beginTime, endTime))
      .cache()
    
    //计算新增用户数：Daily New Users（DNU）
    val dnu = filterLogs.filter(arr => FilterUtils.filterByType(arr, EventType.REGISTER))
    println("日新曾用户："+dnu.count)
    
    
    //日活跃用户：Daily Active Users(DAU)
    //当日注册的和登陆的用户都算日活用户，根据用户名去除重复登录的用户
    val dau = filterLogs.filter(arr => FilterUtils.filterByTypes(arr, EventType.REGISTER, EventType.LOGIN))
      .map(_(3)).distinct()
    println("日活用户：" + dau.count())
    
    
    //次日留存用户
    //首先需要求出前一日新增用户DNU，然后求出第二天登陆的用户d2Login
    //然后用第DNU去join d2Login，求出来的就是次日留存率
    //能join首先需要是key-value，需要将join的数据转换成key-value的格式
    val dnuMap = dnu.map(arr => (arr(3), 1))
    
    val d2Login = splitedLog.filter(arr => FilterUtils.filterByTypeAndTime(arr, EventType.LOGIN, time1, time2))
    //去除重复登陆的数据，转换成（用户名，1）的格式
    val d2LMap = d2Login.map(_(3)).distinct().map((_, 1))
    
    //  留存率：某段时间的新增用户数记为A，经过一段时间后，仍然使用的用户占新增用户A的比例即为留存率
    //  次日留存率（Day 1 Retention Ratio） Retention [rɪ'tenʃ(ə)n] Ratio ['reɪʃɪəʊ]
    //  日新增用户在+1日登陆的用户占新增用户的比例
    val d1rr = dnuMap.join(d2LMap)
    println("次日留存率：" + d1rr.count)
    
    
    sc.stop()
  } 
  
}





















