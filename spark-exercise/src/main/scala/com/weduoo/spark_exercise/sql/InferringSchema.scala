package com.weduoo.spark_exercise.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object InferringSchema {
  def main(args: Array[String]): Unit = {
    val PATH: String = "/Users/weduoo/test/spark";
    //创建sparkConf并设置应用名称
    val conf = new SparkConf().setAppName("InferringSchema").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //通过sc创建一个sqlContext
    val sqlContext = new SQLContext(sc)
    
    //读取数据
    val lineRDD = sc.textFile(PATH + "/sql/person.txt").map(_.split(","))
    
    //创建RDD与case class的关联
    val personRDD = lineRDD.map(x => Person(x(0).toInt, x(1),x(2).toInt) )
    
    //导入隐式转换，否则无法将RDD转换成DataFrame
    import sqlContext.implicits._
    val personDF = personRDD.toDF()
    //注册表
    personDF.registerTempTable("t_person")
    
    //使用sqlContext查询数据
    val df = sqlContext.sql("select * from t_person order by age desc limit 3")
    println(df.show())
    
    //将结果以JSON的形式存储
    df.write.json(PATH + "/out/sql-1")
    sc.stop()
  }
}
//case class 
case class Person(id: Int, name:String, age: Int)