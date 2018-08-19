package com.ibeifeng.bigdata.spark.app.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by wanglonglong on 2018/6/2.
 */
object UDFandUDAF {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    .setMaster("local[*]")
    .setAppName("udaf")
    val sc=SparkContext.getOrCreate(conf)
    val sqlContext=new SQLContext(sc)
    import sqlContext.implicits._
    //UDF定义
    sqlContext.udf.register("format_double",(value:Double)=>{
      //引入BigDecimal
      import java.math.BigDecimal
      val bd=new BigDecimal(value)
      bd.setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue()
    })
  //UDAF注册
    sqlContext.udf.register("self_avg", avgUDAF)

    //构建模拟数据
    sc.parallelize(Array(
      (1, 11234),
      (1, 512312),
      (1, 412334),
      (1, 123124),
      (2, 4132314),
      (2, 1231234),
      (2, 441234),
      (2, 31231234),
      (3, 1234),
      (3, 61234),
      (3, 71234),
      (3, 51234)
    )).toDF("id","sal").registerTempTable("tmp_emp")
    sqlContext.sql(
      """
        |select
        |id,
        |AVG(sal) as sal1,
        |self_avg(sal) as sal2,
        |format_double(self_avg(sal)) as sal3
        |from tmp_emp
        |group by id
      """.stripMargin)
    .show()
  }
}