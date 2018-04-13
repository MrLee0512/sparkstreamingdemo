package com.liz.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Demo1 {


  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("socketStreaming")
    val ssc = new StreamingContext(conf, Seconds(5))

//    ssc.checkpoint("d://ss_checkpoint")

    val lines = ssc.socketTextStream("127.0.0.1", 51212)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(s => (s, 1))

//    pairs.updateStateByKey(updateFunction).print()

    pairs.reduceByKey(_ + _).print()

    pairs.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(15), Seconds(15)).print()

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * updateStateByKey中的Function，用于统计全局的数据数量，需要配合checkpoint使用
    */
  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    var newCount = 0
    if (runningCount.isDefined) {
      newCount += runningCount.get
    }
    for (elem <- newValues) {
      newCount += elem
    }
    Some(newCount)
  }
}
