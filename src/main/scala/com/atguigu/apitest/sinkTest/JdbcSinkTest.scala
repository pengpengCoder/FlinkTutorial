package com.atguigu.apitest.sinkTest

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.apitest.{SensorReading, SensorSource}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.sinkTest
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/14 16:56
  */
object JdbcSinkTest {
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

    val dataStream2 = env.addSource(new SensorSource())

    dataStream2.addSink( new MyJdbcSink() )

    env.execute("jdbc sink test job")
  }
}

// 自定义实现一个 sink function
class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  // 定义出sql连接、预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    // 创建连接
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    // 定义预编译语句
    insertStmt = conn.prepareStatement("INSERT INTO sensor_temp (sensor, temperature) VALUES (?,?)")
    updateStmt = conn.prepareStatement("UPDATE sensor_temp SET temperature = ? WHERE sensor = ?")
  }

  // 每条数据来了之后，调用连接，执行sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    // 先执行更新语句
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    // 如果没有更新数据，那么插入
    if( updateStmt.getUpdateCount == 0 ){
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
