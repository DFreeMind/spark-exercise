package com.weduoo.spark_exercise.game

import org.apache.commons.lang3.time.FastDateFormat

/**
 * 使用单例对象，每个Executor中只有一个FilterUtils实例
 * 但Executor进程中的task是多线程的
 */
object FilterUtils {
  //simpleDateFormat线程不安全，使用FastDateFormat线程安全
  val dateFormat = FastDateFormat.getInstance("yyyy年MM月dd日,E,HH:mm:ss")
  
  //通过时间过滤，过滤某一段时间的数据
  def filterByTime(fields: Array[String], startTime: Long, endTime: Long) = {
    val time = fields(1)
    val longTime = dateFormat.parse(time).getTime
    longTime >= startTime && longTime < endTime
  }
  
  //通过事件类型过滤
  def filterByType(fields: Array[String], eventType: String)= {
    val _type = fields(0)
    _type == eventType
  }
  
  //通过时间与事件类型过滤
  def filterByTypeAndTime(fields: Array[String], eventType: String, beginTime: Long, endTime: Long) = {
    val _type = fields(0)
    val time = fields(1)
    val timeLong = dateFormat.parse(time).getTime
    _type == eventType && timeLong >= beginTime && timeLong < endTime
  }
  
  //根据多种事件类型过滤，符合其中的一种就保留
  def filterByTypes(fields: Array[String], eventTypes: String*):Boolean = {
    val _type = fields(0)
    for(et <- eventTypes){
      if(_type == et){
        return true
      }
    }
    false
  }
}


