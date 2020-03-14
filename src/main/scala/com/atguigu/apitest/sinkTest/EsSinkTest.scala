package com.atguigu.apitest.sinkTest

import java.util

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.sinkTest
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/14 16:31
  */
object EsSinkTest {
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

    // 定义一个HttpHost的List
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))
    // 创建一个esSink的Builder
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("saving data: " + t)
          // 从数据中提取信息，包装成 index request，通过requestIndexer发送出去
          // 1. 定义data source
          val dataSource = new util.HashMap[String, String]()
          dataSource.put("sensor_id", t.id)
          dataSource.put("temperature", t.temperature.toString)
          dataSource.put("ts", t.timestamp.toString)
          // 2. 创建 index request
          val indexRequest = Requests.indexRequest()
            .index("sensor")
            .`type`("readingdata")
            .source(dataSource)
          // 3. 用 requestIndexer发送http请求
          requestIndexer.add(indexRequest)
          println("saved successfully")
        }
      }
    )

    dataStream.addSink( esSinkBuilder.build() )

    env.execute("es sink test job")
  }
}
