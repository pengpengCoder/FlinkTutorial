package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.wc
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/13 11:51
  */

// 流处理 word count
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//    env.disableOperatorChaining()
    // 2. 从 socket 文本流中读取数据
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")
    val inputStream: DataStream[String] = env.socketTextStream(host, port)
    // 3. 对数据进行处理，计算word count
    val resultStream: DataStream[(String, Int)] = inputStream
      .flatMap( _.split(" ") )
      .filter( _.nonEmpty ).startNewChain()
      .map( (_, 1) )
      .keyBy(0)     // 指定以二元组中第一个元素，也就是word作为key，然后按照key分组
      .sum(1)

    // 4. 打印输出
    resultStream.print().setParallelism(1)

    // 启动执行任务
    env.execute()
  }
}
