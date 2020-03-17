package com.atguigu.apitest

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/16 16:59
  */
object ProcessFuntionTest {
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

    val warningStream = dataStream
      .keyBy("id")
      .process( new TempIncreWarning(5000L) )

    warningStream.print()
    env.execute()
  }
}

// 自定义 Process Function，用来定义定时器，检测温度连续上升
class TempIncreWarning(interval: Long) extends KeyedProcessFunction[Tuple, SensorReading, String]{
  // 把上一次的温度值，保存成状态
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  // 把注册的定时器时间戳保存成状态，方便删除
  lazy val curTimerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("cur-timer", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 先把状态取出来
    val lastTemp = lastTempState.value()
    val curTimerTs = curTimerState.value()

    lastTempState.update(value.temperature)

    // 判断：如果温度上升并且没有定时器，那么注册定时器
    if( value.temperature > lastTemp && curTimerTs == 0 ){
      val ts = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerProcessingTimeTimer(ts)
      // 保存ts到状态
      curTimerState.update(ts)
    } else if( value.temperature < lastTemp ){
      // 如果温度值下降，直接删除定时器
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      // 清空状态
      curTimerState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 定时器触发，报警
    out.collect("传感器"+ ctx.getCurrentKey + "温度连续5秒上升")
    // timer状态清空
    curTimerState.clear()
  }
}
