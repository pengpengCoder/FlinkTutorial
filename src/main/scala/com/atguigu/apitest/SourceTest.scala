package com.atguigu.apitest

import java.util.Properties

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/14 10:28
  */

// 定义样例类
case class SensorReading( id: String, timestamp: Long, temperature: Double )

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // Source1: 从集合中读取数据
    val stream1: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    ))
//    env.fromElements(1, 0.4, "hello")
    // Source2: 从文件中读取数据
    val stream2: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // Source3: 从 Kafka中读取数据
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.getProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream3 = env.addSource( new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties) )

    // Source4: 自定义source，随机生成测试数据
    val stream4 = env.addSource( new SensorSource() )

    // 打印输出
//    stream1.print("stream1")
//    stream2.print("stream2")
//    stream3.print("stream3")
    stream4.print("stream4")
    env.execute()
  }
}

// 自定义 source function
class SensorSource() extends SourceFunction[SensorReading]{
  // 定义一个运行标志位
  var running: Boolean = true

  override def cancel(): Unit = running = false

  // 通过上下文ctx，连续不断地生成数据，发送到流里
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 初始化一个随机数发生器
    val rand = new Random()
    // 随机生成10个传感器的温度值初始值
    var curTemp = 1.to(10).map(
      // 转换成(sensorId, temperature)二元组
      i => ( "sensor_" + i, 60 + rand.nextGaussian() * 20 )
    )

    // 无限循环，产生数据
    while( running ){
      // 每个传感器各自随机更新温度值
      val curTime = System.currentTimeMillis()
      curTemp = curTemp.map(
         data => (data._1, data._2 + rand.nextGaussian())
       )
      // 利用ctx发送所有传感器的数据
      curTemp.foreach(
        data => ctx.collect( SensorReading(data._1, curTime, data._2) )
      )
      // 间隔1s
       Thread.sleep(1000)
    }
  }
}