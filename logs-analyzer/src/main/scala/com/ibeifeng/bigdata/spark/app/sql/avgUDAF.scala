package com.ibeifeng.bigdata.spark.app.sql

import org.apache.spark.sql.{types, Row}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
 * Created by wanglonglong on 2018/6/2.
 */
object avgUDAF extends UserDefinedAggregateFunction{
  def main(args: Array[String]) {

  }

  override def inputSchema: StructType = {
    StructType(Array(
    StructField("iv",DoubleType)
    ))
  }

  override def bufferSchema: StructType = {
    StructType(Array(
    StructField("tv",DoubleType),
    StructField("tc",IntegerType)
    ))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //当前两个分区合并时调用
    //获取buffer1数据
    val tv1=buffer1.getDouble(0)
    val tc1=buffer1.getInt(1)
    //获取buffer2数据
    val tv2=buffer2.getDouble(0)
    val tc2=buffer2.getInt(1)
    //更新buffer1数据
    buffer1.update(0,tv1+tv2)
    buffer1.update(1,tc1+tc2)
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //初始化缓存数据的初始值
    buffer.update(0,0.0)
    buffer.update(1,0)
  }

  override def update(buffer: MutableAggregationBuffer,input:Row): Unit ={
    //对于一条输入数据，更新buffer的值
    //1.获取输入数据
    val iv=input.getDouble(0)
    //2.获取缓存区数据
    val tv=buffer.getDouble(0)
    val tc=buffer.getInt(1)
    //3.更新缓存区数据
    buffer.update(0,tv+iv)
    buffer.update(1,tc+1)

  }
  override def deterministic: Boolean ={
    true
  }

  override def evaluate(buffer: Row): Any ={
    buffer.getDouble(0)/buffer.getInt(1)
  }

  override def dataType: DataType = {
  DoubleType
  }
}
