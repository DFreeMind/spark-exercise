package com.weduoo.spark_exercise.game

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * 处理时间的工具类
 */
object TimeUtils {
  
  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val calendar = Calendar.getInstance()
  
  //apply方法，不用new，调用的是此方法
  def apply(time:String) = {
    calendar.setTime(simpleDateFormat.parse(time))
    //获取时间的毫秒值
    calendar.getTimeInMillis
  }
  
  def getCertainDayTime(amount: Int):Long = {
    calendar.add(Calendar.DATE, amount)
    val time = calendar.getTimeInMillis
    calendar.add(Calendar.DATE, -amount)
    time
  }
}