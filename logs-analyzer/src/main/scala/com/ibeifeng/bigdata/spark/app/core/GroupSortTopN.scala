package com.ibeifeng.bigdata.spark.app.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by wanglonglong on 2018/5/29.
 */
object GroupSortTopN {
  def main(args: Array[String]) {
    val k=2
   val conf=new SparkConf()
    .setMaster("local")
    .setAppName("topN")
    val sc=new SparkContext(conf)
    val path="data/groupsort.txt"
    val rdd=sc.textFile(path)
    .map(_.split(" "))
    .filter(arr=>arr.length==2)
    .map(arr=>(arr(0),arr(1).toInt))
    .groupByKey
    .flatMap{
      case (item1,iter)=>{
        val topN=iter
        .toList
        .sorted
        .takeRight(k)
        topN.map(item2=>(item1,item2))
      }
    }
    rdd.foreachPartition(out=>out.foreach(println))
  }
}
