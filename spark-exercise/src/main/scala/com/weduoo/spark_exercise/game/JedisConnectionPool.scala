package com.weduoo.spark_exercise.game

import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.Jedis

/**
 * redis连接池工具
 */
object JedisConnectionPool {
  val config: JedisPoolConfig = new JedisPoolConfig()
  //最大连接数
  config.setMaxTotal(10)
  //最大空闲连接数
  config.setMaxIdle(5)
  //当调用borrow Object方法时，是否进行有效性检查（此版本暂时不可用）
  config.setTestOnBorrow(true)
  config.setMaxWaitMillis(30);
  
  val pool = new JedisPool(config, "192.168.11.101", 6379)
  
  def getConnection(): Jedis = {
    pool.getResource
  }
  //连接测试
  def main(args: Array[String]): Unit = {
    val conn = JedisConnectionPool.getConnection()
    val r = conn.keys("*")
    println(r)
  }
}