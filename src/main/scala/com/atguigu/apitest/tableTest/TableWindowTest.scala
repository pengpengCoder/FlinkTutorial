package com.atguigu.apitest.tableTest

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table, Tumble}
import org.apache.flink.table.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.tableTest
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/18 15:48
  */
object TableWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 读取数据
    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    // 先创建一个table执行环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 基于流创建一个表
    val dataTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime)

    // 定义转换操作，开一个10秒的滚动窗口，统计窗口内每个id对应的数量
    val resultTable: Table = dataTable
      .window( Tumble over 10.seconds on 'timestamp as 'tw)
      .groupBy('id, 'tw)
      .select('id, 'id.count)

    // 直接写sql做转换
    val resultSqlTable: Table = tableEnv.sqlQuery("select id, count(id) from "
    + dataTable + " group by id,tumble(`timestamp`, interval '10' second)")

    // 结果表转换成流输出
    val resultStream: DataStream[(Boolean, (String, Long))] = resultSqlTable.toRetractStream[(String, Long)]
      .filter(_._1)

    resultStream.print()
    env.execute("table window test")
  }
}
