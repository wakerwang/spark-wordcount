
package com.ibeifeng.bigdata.spark.app.Streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by wanglonglong on 2018/7/9.
 */
object UseDirectStreamingkafkaWORDCount {
  def main (args: Array[String]) {
    val conf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("UseDirectStreamingkafkaWORDCount")
    val sc=SparkContext.getOrCreate(conf)
    val ssc=new StreamingContext(sc,Seconds(10))
    //创建数据
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "bigdata.wll01.com:9092,bigdata.wll01.com:9093,bigdata.wll01.com:9094,bigdata.wll01.com:9095",
      "auto.offset.reset" -> "largest")
    val topics= Set("beifeng1")
    val dstream =KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
    //dstream操作
    val resultDstream=dstream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
    resultDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
