package com.ibeifeng.bigdata.spark.app.Streaming

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by wanglonglong on 2018/7/9.
 */
object UseReceiveStreamingkafkaWORDCount {
  def main (args: Array[String]) {
    val conf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("UseReceiveStreamingkafkaWORDCount")
    val sc=SparkContext.getOrCreate(conf)
    val ssc=new StreamingContext(sc,Seconds(10))
    //创建数据
    val kafkaParams = Map[String, String](
      "meta" -> "bigdata.wll01.com:2181/kafka13",
      "group.id" -> "streaming01",
      "zookeeper.connection.timeout.ms" -> "10000")
    val topics= Map("beifeng1"->3)
    val dstream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER_2).map(_._2)
    //dstream操作
    val resultDstream=dstream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
    resultDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}