package com.weduoo.spark_exercise.game

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.elasticsearch.spark._


/**
 * ElasticSearch整合spark
 * 1、需要导入ElasticSearch核心jar包
 * 2、导入ElasticSearch Spark插件包
 */
object ElasticSpark {
  def main(args: Array[String]): Unit = {
    val CLUSTER = "192.168.11.101,192.168.11.102,192.168.11.103"
    val conf = new SparkConf().setAppName("ElasticSpark").setMaster("local[2]")
    //ES配置，节点地址
    conf.set("es.nodes", CLUSTER)
    //端口号
    conf.set("es.port", "9200")
    //是否自动创建index
    conf.set("es.index.auto.create", "true")
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    
    
    val EventType:Int = 4
    val query: String = 
      s"""{
        "query":{"match_all":{}},
        "filter" : {
          "bool" : {
            "must" : [
              {"term" : {"event_type" : $EventType}}
            ]
          }
        } 
        }"""
    //使用es上的RDD          
    val rdd1 = sc.esRDD("level-one-2017.06.14",query)
    
    println(rdd1.collect().toBuffer)
    println(rdd1.collect().size)
    
    
  }
}