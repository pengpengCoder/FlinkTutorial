package com.atguigu.apitest.tableTest

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.tableTest
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/18 15:30
  */
object TableTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取数据
    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 先创建一个table执行环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
        .useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 基于流创建一个表
    val dataTable: Table = tableEnv.fromDataStream(dataStream)

    // 定义转换操作
    val resultTable: Table = dataTable
      .select("id, temperature")
      .filter("id = 'sensor_1'")

    // 结果表转换成流输出
    val resultStream: DataStream[(String, Double)] = resultTable.toAppendStream[(String, Double)]

    resultStream.print()
    env.execute("table api test")
  }
}
