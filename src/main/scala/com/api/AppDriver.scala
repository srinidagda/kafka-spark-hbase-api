package com.api

import java.util.UUID

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Logger, Level}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object AppDriver {
  def main(args: Array[String]): Unit = {
    val spark = new SparkConf().setAppName("cfd-scala-kafka-spark-hbase-api")
      .setMaster(args(0))
    val BROKER:String = args(1)
    val GROUP_ID:String = UUID.randomUUID().toString
    val sparkStreaming = new StreamingContext(spark,Durations.minutes(1))
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)
    val kafkaParams = Map[String, Object](ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> BROKER,
      ConsumerConfig.GROUP_ID_CONFIG -> GROUP_ID, ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer] ,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer], ConsumerConfig.AUTO_OFFSET_RESET_CONFIG->"earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: java.lang.Boolean)
    )
    val topics = Array(args(2))
    /* val stream = KafkaUtils.createDirectStream[String, String] (sparkStreaming,
       PreferConsistent,
       Subscribe[String, String](topics, kafkaParams))*/

    val streaming = KafkaUtils.createDirectStream[String, String](
      sparkStreaming,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val dStream = streaming.map(lines => lines.value())
    dStream.print()
    sparkStreaming.start()
    sparkStreaming.awaitTermination()
    sparkStreaming.stop()
  }
}