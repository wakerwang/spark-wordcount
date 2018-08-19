package com.ibeifeng.bigdata.spark.app.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by wanglonglong on 2018/6/2.
 */
object ReadCsv {
  def main(args: Array[String]) {
    val conf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("udaf")
    val sc=SparkContext.getOrCreate(conf)
    val sqlContext=new HiveContext(sc)
    import sqlContext.implicits._
    //读取csv文件,将其注册为临时表
    val schema=StructType(Array(
    StructField("id",IntegerType),
      StructField("lat",StringType),
      StructField("lon",StringType),
      StructField("time",StringType)
    ))
    val path="data/taxi.csv"
    sqlContext
    .read
    .format("com.databricks.spark.csv")
    .option("header","false")
    .schema(schema)
    .load(path)
    .registerTempTable("tmp_taxi")
    //3.获取给定格式的属性数据，并将其注册为临时表
    sqlContext.sql(
      """
        |SELECT
        | id,substring(time,0,2) as hour
        | from tmp_taxi
      """.stripMargin)
          .registerTempTable("tmp_id_hour")
    //计算各个小时 各个出租车的载客次数
    sqlContext.sql(
      """
        |select
        | id,hour,count(1) as count
        |from tmp_id_hour
        |group by id,hour
      """.stripMargin)
    .registerTempTable("tmp_id_hour_count")

  //计算各个时间段中载客次数最多的前五
    sqlContext.sql(
      """
        |select
        |id,hour,count,
        |row_number() over (partition by hour order by count desc) as rnk
        |from tmp_id_hour_count
      """.stripMargin)
    .registerTempTable("tmp_id_hour_count_rnk")
    val resultDF=sqlContext
    .sql(
      """
        |select
        |id,hour,count
        |from tmp_id_hour_count_rnk
        |where rnk<=5
      """.stripMargin)

    //结果输出
    resultDF.show()
    resultDF
      .repartition(1)
    .write
    .format("com.databricks.spark.csv")
    .option("header","true")
    .mode(SaveMode.Overwrite)
    .save("data/sql/csv/01")
    Thread.sleep(100000)
  }
}
