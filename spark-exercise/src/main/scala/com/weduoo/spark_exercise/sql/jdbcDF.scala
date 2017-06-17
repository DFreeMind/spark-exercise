package com.weduoo.spark_exercise.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import java.util.Properties
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext

object jdbcDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("jdbcDF").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqc = new SQLContext(sc)
    //通过并行化创建RDD
    var data = Array("5 kakashi 35","6 sasuke 31","7 jiraya 56")
    val personRDD = sc.parallelize(data).map(_.split(" "))
    //通过StructType直接指定每个字段的schema
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )
    //将RDD映射到rowRDD
    val rowRDD = personRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).toInt))
    //将schema信息应用到rowRDD上
    val personDF = sqc.createDataFrame(rowRDD, schema)
    //创建Properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "123456")
    //将数据追加到数据库
    val jdbcUrl = "jdbc:mysql://192.168.11.101:3306/spark"
    personDF.write.mode("append").jdbc(jdbcUrl, "person", prop)
    //停止SparkContext
    sc.stop()
  }
}