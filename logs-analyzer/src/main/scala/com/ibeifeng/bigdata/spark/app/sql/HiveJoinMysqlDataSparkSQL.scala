package com.ibeifeng.bigdata.spark.app.sql

import java.util.Properties

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 *
 * 需要使用到HIVE元数据
 * Created by wanglonglong on 2018/6/2.
 */
object HiveJoinMysqlDataSparkSQL {
  def main(args: Array[String]): Unit = {
    //定义常量信息

    //val driver="com.mysql.jdbc.Driver"
    val url="jdbc:mysql://hostname:3306/test"
    val password="123456"
    val user="root"
    val props=new Properties()
    props.put("password",password)
    //创建上下文
    val conf=new SparkConf()
    .setMaster("local[*]")
    .setAppName("hive-join-mysql")
    val sc=SparkContext.getOrCreate(conf)
    val sqlContext=new HiveContext(sc)
    //需求1：将hive中数据同步到MySQL中
    sqlContext
    .read
    .table("db_emp.dept")
    .write
      .mode(SaveMode.Overwrite)
    .jdbc(url,"tb_dept",props)

  }
}
