package com.liz.spark.streaming

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.streaming._

/**
  * kafka数据写入es demo
  * Created by liz on 2018/5/21 15:38
  */
object Data2ES {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("sparkstreaming2es")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "bigdata")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    val kafkaParam = Map[String, Object] (
      "bootstrap.servers" -> "bigdata:9092",
      "group.id" -> "group-0601",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer]
    )

    val topic = Array("testTopic")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topic, kafkaParam)
    )


//    val spark = SparkSession.builder().config(conf).getOrCreate()
//    stream.foreachRDD(rdd => {
//      val schema = StructType(
//        Array(
//          StructField("index", StringType, false),
//          StructField("plateNumber", StringType, false),
//          StructField("brand", IntegerType, false),
//          StructField("model", IntegerType, false),
//          StructField("year", IntegerType, false),
//          StructField("captureTime", LongType, false)
//        ))
//      val rows = rdd.mapPartitions(msg => {
//        msg.map(m => {
//          println(s"offset: ${m.offset()}, partition: ${m.partition()}, ${m.value()}")
//          val arr = m.value().split("_")
//          Row(arr(0), arr(1), arr(2).toInt, arr(3).toInt, arr(4).toInt, arr(5).toLong)
//        })
//      })
//
//      val df = spark.createDataFrame(rows, schema)
//      EsSparkSQL.saveToEs(df, "test_index0614/test_type", Map("es.mapping.id" -> "index"))
//
//      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
//    })

    stream.mapPartitions(rdds => {
      rdds.map(rdd => {
        val arr = rdd.value().split("_")
        println(s"offset: ${rdd.offset()}, partition: ${rdd.partition()}, ${rdd.value()}")

        new JSONObject()
          .fluentPut("index", arr(0))
          .fluentPut("plateNumber", arr(1))
          .fluentPut("brand", arr(2))
          .fluentPut("model", arr(3))
          .fluentPut("year", arr(4))
          .fluentPut("captureTime", arr(5))
          .toJSONString
      })
    }).saveJsonToEs("test_index0614/test_type", Map("es.mapping.id" -> "index"))

    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
