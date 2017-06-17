package com.weduoo.spark_exercise.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row


object StructSchema {
  
  def main(args: Array[String]): Unit = {
    val PATH: String = "/Users/weduoo/test/spark";
    
    val conf = new SparkConf().setAppName("StructSchema").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqc = new SQLContext(sc)
    
     val lineRDD = sc.textFile(PATH + "/sql/person.txt").map(_.split(","))
     
     //通过StructType指定Schema
     val schema = StructType(List(
         StructField("id", IntegerType, true),
         StructField("name", StringType, true),
         StructField("age", IntegerType, true)
      ))
      
      //将RDD映射到RowRDD
      val rowRDD = lineRDD.map(p => Row(p(0).toInt,p(1),p(2).toInt))
      //将schema信息应用到rowRDD上
      val personDF = sqc.createDataFrame(rowRDD, schema)
      
      //注册表
    personDF.registerTempTable("t_person")
    
    //使用sqlContext查询数据
    val df = sqc.sql("select * from t_person order by age desc limit 3")
    println(df.show())
    
    //将结果以JSON的形式存储
    df.write.json(PATH + "/out/sql-struct")
    sc.stop()
  }
}










