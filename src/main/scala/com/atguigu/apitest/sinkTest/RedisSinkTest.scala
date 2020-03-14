package com.atguigu.apitest.sinkTest

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.sinkTest
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/14 16:11
  */
object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取数据
    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // 基本转换操作，转换成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 向redis里写入数据
    val config = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    dataStream.addSink( new RedisSink[SensorReading](config, new MyRedisMapper()) )

    env.execute("redis sink test job")
  }
}

// 自定义RedisMapper
class MyRedisMapper() extends RedisMapper[SensorReading]{
  // 保存数据到redis的命令定义，保存成一张hash表，HSET sensor_temp id temperature
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
  }

  override def getValueFromData(t: SensorReading): String = t.temperature.toString

  override def getKeyFromData(t: SensorReading): String = t.id
}