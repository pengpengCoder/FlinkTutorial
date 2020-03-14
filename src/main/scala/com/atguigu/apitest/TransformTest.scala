package com.atguigu.apitest

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/14 11:48
  */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取数据
    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // 1. 基本转换操作，转换成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
    // 2. keyBy之后做聚合，获取到目前为止的温度最小值
    val aggStream: DataStream[SensorReading] = dataStream
      .keyBy(_.id)
//      .minBy("temperature")
      .reduce(
      (curState, newData) =>
        SensorReading(curState.id, newData.timestamp, newData.temperature.min(curState.temperature))
    )

    // 3. 分流，按照温度值高低拆分
    val splitStream: SplitStream[SensorReading] = dataStream.split(
      sensorData => {
        if( sensorData.temperature > 30 )
          Seq("highTemp")
        else
          Seq("lowTemp")
      }
    )
    // 调用select得到对应的DataStream
    val highTempStream: DataStream[SensorReading] = splitStream.select("highTemp")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("lowTemp")
    val allTempStream = splitStream.select("highTemp", "lowTemp")

    // 4. 合流，connect连接两条类型不同的流
    val warningStream: DataStream[(String, Double)] = highTempStream
      .map( data => (data.id, data.temperature) )
    val connectedStreams: ConnectedStreams[(String, Double), SensorReading] = warningStream
      .connect(lowTempStream)
    val resultStream: DataStream[Serializable] = connectedStreams.map(
      warningData => (warningData._1, warningData._2, "high temp warning"),
      lowTempData => (lowTempData.id, "healthy")
    )

    // 5. 合并两条流：Union
    val unionStream = highTempStream.union(lowTempStream, allTempStream)

    // 6. 自定义函数类
    val transformStream1 = dataStream.map(new MyMapper)
    val transformStream2 = dataStream.filter(new MyFilter("sensor_1"))

//    aggStream.print()
//    highTempStream.print("high")
//    lowTempStream.print("low")
//    allTempStream.print("all")
    resultStream.print()

    env.execute()
  }
}


// 自定义一个MapFunction
class MyMapper extends MapFunction[SensorReading, Int]{
  override def map(value: SensorReading): Int = {
    value.temperature.toInt
  }
}
// 自定义一个FilterFunction，筛选出以sensor_1开头的数据
class MyFilter(keyWord: String) extends FilterFunction[SensorReading]{
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith(keyWord)
  }
}

// 自定义一个富函数类
class MyRichMapper extends RichMapFunction[Double, Int]{
  var subTaskIndex = 0

  override def open(parameters: Configuration): Unit = {
    subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
  }

  override def map(value: Double): Int = value.toInt

  override def close(): Unit = {
  }
}