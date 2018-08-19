package com.ibeifeng.bigdata.spark.app.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by wanglonglong on 2018/5/29.
  */
object GroupSortTopN02 {
  val random = Random
   def main(args: Array[String]) {
     val k: Int = 2
     val path = "data/groupsort.txt"
     val conf=new SparkConf()
     .setMaster("local")
     .setAppName("topK")
     val sc=new SparkContext(conf)
     val rdd=sc.textFile(path)
     .map(_.split(" "))
     .filter(arr=>arr.length==2)
     .map(arr=>(arr(0),arr(1).toInt))
     .aggregateByKey(ArrayBuffer[Int]())(
          (u, v) => {
           u += v
         },
          (u1, u2) => {
           u1 ++= u2
           u1.sorted.takeRight(k)
         }
       )
     val outRDD=rdd.map(arr=>())
     rdd.foreachPartition(arr=>arr.foreach(println))
   }
 }
