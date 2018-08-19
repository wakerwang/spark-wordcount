package com.ibeifeng.bigdata.spark.app.sql

/**
 * Created by wanglonglong on 2018/6/3.
 */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
object SparkStreaming {
  def main(args: Array[String]) {
    val conf=new SparkConf()
    .setMaster("local[*]")
    .setAppName("test")
    val sc=SparkContext.getOrCreate(conf)
    val ssc=new StreamingContext(sc,Seconds(10))
    val dstream=ssc.socketTextStream("bigdata.wll01.com",9999)
    val result=dstream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
    result.print()
    ssc.start()
    ssc.awaitTermination()
   /* val ssc=new StreamingContext(sc,Milliseconds(500))

    val dstream=ssc.socketTextStream("bigdata.wll01.com",9999)
    val resultDstream=dstream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
    resultDstream.print()
    ssc.start()
    ssc.awaitTermination()*/

  }

}
