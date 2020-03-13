package com.atguigu.wc

import org.apache.flink.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.wc
  * Version: 1.0
  *
  * Created by wushengran on 2020/3/13 11:32
  */

// 批处理 word count
object WordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建一个批处理执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 2. 从文件中读取数据
    val inputPath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)
    // 3. 对数据进行处理，计算 word count: (word, count)
    val resultDataSet: DataSet[(String, Int)] = inputDataSet
      .flatMap( _.split(" ") )     // 先把每一行数据按照空格分词
      .map( (_, 1) )    // 包装成(word, count)二元组
      .groupBy(0)    // 基于二元组中的第一个元素，也就是word做分组
      .sum(1)    // 将每组中按照二元组的第二个元素，也就是count做聚合

    // 4. 打印输出
    resultDataSet.print()
  }
}
