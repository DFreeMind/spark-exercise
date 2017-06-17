package com.weduoo.spark_exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


/**
 * 使用隐式转换方式
 */
case class Girl(faceValue: Int, age: Int) extends Serializable
object OrderContext {
  implicit val girlOrdering  = new Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = {
      if(x.faceValue > y.faceValue) 1
      else if (x.faceValue == y.faceValue) {
        if(x.age > y.age) -1 else 1
      } else -1
    }
  }
}

object CustomSort {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CustomSort").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("weduoo", 90, 24, 1), ("sakura", 90, 27, 2),
        ("hinata", 95, 22, 3)))
    import OrderContext._
    val rdd2: RDD[(String, Int, Int, Int)] = 
        rdd1.sortBy(x => Girl(x._2, x._3), false)
    println(rdd2.collect().toBuffer)
    sc.stop()
  }
}

/**
 * 使用对象封装数据
 */
//case class Girl(val faceValue: Int, val age: Int) 
//    extends Ordered[Girl] with Serializable {
//  override def compare(that: Girl): Int = {
//    if(this.faceValue == that.faceValue) {
//      that.age - this.age
//    } else {
//      this.faceValue -that.faceValue
//    }
//  }
//}






