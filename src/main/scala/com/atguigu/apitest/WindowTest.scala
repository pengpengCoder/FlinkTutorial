package com.atguigu.apitest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/16 9:34
  */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(300L)

    // 读取数据
//    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // 基本转换操作，转换成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
//      .assignAscendingTimestamps( _.timestamp * 1000L )
//      .assignTimestampsAndWatermarks(new MyPeriodicAssigner(1000L))
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          // 从数据中提取时间戳
          element.timestamp * 1000L
        }
      } )

//    dataStream.keyBy("id")
//      .window( SlidingProcessingTimeWindows.of(Time.hours(1), Time.seconds(1), Time.hours(-8)) )
//      .countWindow(10, 2)
//      .timeWindow(Time.hours(1))
//      .trigger()
//      .evictor()
//      .allowedLateness(Time.minutes(10))
//      .sideOutputLateData()
//      .sum(2)
//      .getSideOutput()
//      .apply( new MyWindowFunction() )

    // 输出每15秒每个传感器温度的最小值
    val minTempPerWinStream: DataStream[(String, Double)] = dataStream
      .map( data => (data.id, data.temperature) )
      .keyBy(_._1)    // 按照sensor id分组
//      .timeWindow(Time.seconds(15))    // 15秒的滚动窗口
      .timeWindow(Time.seconds(15), Time.seconds(5))    // 15秒的滑动窗口，滑动步长5秒
//      .reduce( (curMin, newData) => (curMin._1, curMin._2.min(newData._2)) )
      .reduce( new MyReduceFunction() )

    minTempPerWinStream.print()
    env.execute("window test job")
  }
}

// 自定义一个 window function
class MyWindowFunction() extends WindowFunction[SensorReading, String, Tuple, TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[SensorReading], out: Collector[String]): Unit = {
    out.collect(key + " " + window.getStart)
  }
}

// 自定义一个增量聚合函数
class MyReduceFunction() extends ReduceFunction[(String, Double)]{
  override def reduce(value1: (String, Double), value2: (String, Double)): (String, Double) = {
    ( value1._1, value1._2.min(value2._2) )
  }
}

// 自定义一个周期性生成Watermark的assigner
class MyPeriodicAssigner(bound: Long) extends AssignerWithPeriodicWatermarks[SensorReading]{
  // 定义当前最大时间戳
  var maxTs = Long.MinValue

  override def getCurrentWatermark: Watermark = new Watermark(maxTs - bound)

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp * 1000L)
    element.timestamp * 1000L
  }
}

// 自定义可间断地生成watermark的 assiger
class MyPuntuatedAssigner extends AssignerWithPunctuatedWatermarks[SensorReading]{
  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if(lastElement.id == "sensor_1"){
      new Watermark(extractedTimestamp)
    } else{
      null
    }
  }
  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp * 1000L
  }
}