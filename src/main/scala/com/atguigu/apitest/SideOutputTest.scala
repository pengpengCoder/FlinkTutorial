package com.atguigu.apitest

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/17 10:57
  */
object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // 基本转换操作，转换成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 利用process function得到侧输出流，做分流操作
    val highTempStream = dataStream
      .process( new SplitTemp(30.0) )

    // 获取侧输出流数据
    val lowTempStream = highTempStream.getSideOutput(new OutputTag[(String, Double, Long)]("low-temp"))

    highTempStream.print("high")
    lowTempStream.print("low")
    env.execute()
  }
}

// 自定义Process Function，实现具体的分流功能
class SplitTemp(threshold: Double) extends ProcessFunction[SensorReading, SensorReading]{
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    // 判断温度值，是否小于阈值，如果小于输出到侧输出流
    if( value.temperature < threshold ){
      ctx.output(new OutputTag[(String, Double, Long)]("low-temp"), (value.id, value.temperature, value.timestamp))
    } else {
      out.collect(value)
    }
  }
}