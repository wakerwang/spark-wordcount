package com.ibeifeng.bigdata.spark.app.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by wanglonglong on 2018/5/27.
 */
object SparkWordCount {
 def main(args: Array[String]) {
   val conf=new SparkConf()
     .setAppName("wordCount")
     .setMaster("local")
   val sc=new SparkContext(conf)
   val path="C://Users//wanglonglong//Desktop//word.txt"
   // val path="hdfs://bigdata.wll01.com:8020/wang/spark/data/word.txt"
   /// val savePath=s"hdfs://bigdata.wll01.com:8020/wang/spark/core/wc/${System.currentTimeMillis()}"

   val rdd=sc.textFile(path)
     .flatMap(_.split(" "))
     .map(word=>(word,1))
     .reduceByKey(_+_)
   rdd.foreachPartition(out=>out.foreach(println))
   /* val rdd: RDD[String] =sc.textFile(path)
    val wordCountRDD= rdd
    .flatMap(line=>line.split(" "))
    .map(word=>(word,1))
    .reduceByKey(_ + _)
    wordCountRDD.foreachPartition(iter=>iter.foreach(println))
   // wordCountRDD.saveAsTextFile(savePath)*/
  }
}
