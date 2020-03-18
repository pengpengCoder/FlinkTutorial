package com.atguigu.apitest

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/17 14:38
  */
object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 状态后端配置
//    env.setStateBackend(new FsStateBackend(""))
//    env.setStateBackend( new RocksDBStateBackend("") )

    // checkpoint配置
    env.enableCheckpointing(10000L)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(20000L)
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)

    // 重启策略的配置
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L))
//    env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // 基本转换操作，转换成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 检测传感器温度，如果连续两次的温度差值超过10度，输出报警
    val warningStream = dataStream
      .keyBy("id")
//      .flatMap( new TempChangeWarning(10.0) )
      .flatMapWithState[(String, Double, Double), Double]({
      case (inputData: SensorReading, None) => ( List.empty, Some(inputData.temperature) )
      case (inputData: SensorReading, lastTemp: Some[Double]) => {
        val diff = (inputData.temperature - lastTemp.get).abs
        if( diff > 10.0 ){
          (List((inputData.id, lastTemp.get, inputData.temperature)), Some(inputData.temperature))
        } else
          (List.empty, Some(inputData.temperature))
      }
    })

    warningStream.print()
    env.execute()
  }
}

// 自定义一个富函数，如果符合报警信息，输出（id, lastTemp, curTemp）
class TempChangeWarning(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{
  // 先声明状态，用来保存上一次的温度值
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 先读取状态
    val lastTemp = lastTempState.value()

    // 与当前最新的温度值求差值，比较判断
    val diff = (value.temperature - lastTemp).abs
    if( diff > threshold ){
      out.collect( (value.id, lastTemp, value.temperature) )
    }

    // 更新状态
    lastTempState.update(value.temperature)
  }
}

// 自定义函数类，应用operator state
//class MyProcessFunction extends ProcessFunction[SensorReading, String] with ListCheckpointed[Long]{
//  private var myState: Long = _
//
//  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, String]#Context, out: Collector[String]): Unit = ???
//
//  override def restoreState(state: util.List[Long]): Unit = ???
//
//  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[Long] = ???
//}