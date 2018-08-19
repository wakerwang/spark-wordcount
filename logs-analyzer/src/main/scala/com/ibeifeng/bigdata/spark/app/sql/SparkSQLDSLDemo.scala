package com.ibeifeng.bigdata.spark.app.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by wanglonglong on 2018/6/3.
 */
case class Person(name:String,age:Int,sex:String,salary:Int,deptNo:Int)
case class Dept(deptNo:Int,deptName:String)
object SparkSQLDSLDemo {
  def main(args: Array[String]) {
    //创建上下文
    val conf = new SparkConf()
      .setAppName("demo")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._
    sqlContext.udf.register("sexToNum",(sex:String)=>{
      sex.toUpperCase match {
        case "M"=>0
        case "F"=>1
        case  _ => -1
      }
    })

    //创建模拟数据
    val rdd1 = sc.parallelize(Array(
      Person("张三", 21, "M", 16865, 1),
      Person("李四", 22, "F", 1665, 2),
      Person("王五", 23, "M", 6865, 2),
      Person("小明", 24, "F", 6865, 1),
      Person("小李", 25, "M", 1865, 2),
      Person("leo", 25, "F", 1665, 2),
      Person("小胡", 26, "M", 1680, 1),
      Person("tom", 27, "F", 1525, 1),
      Person("小黄", 28, "M", 1274, 2),
      Person("小赵", 29, "M", 1752, 2)
    ))
    val rdd2=sc.parallelize(Array(
    Dept(1,"部门1"),
    Dept(2,"部门2")
    ))

    val personDataFrame=rdd1.toDF()
    val deptDataFrame=rdd2.toDF()
    //==================DSL================================
    //对于多次使用的进行持久化
    personDataFrame.cache()
    deptDataFrame.cache()
    //select
    println("---select----")
    personDataFrame.select("name","age","sex").show()
    personDataFrame.select($"name".as("姓名"),$"age",$"sex").show()
    personDataFrame.selectExpr("name","age","sex","sexToNum(sex) as sex_num").show()
    //where/filter
    println("---where/filter----")
    personDataFrame.where("age>22").where("sex='M'").where("deptNo=1").show()
    personDataFrame.where("age >20 AND sex='M' AND deptNo=1").show()
    personDataFrame.where($"age" > 20 && $"sex"==="M" && $"deptNo"===1).show()
    println("---sort----")
    personDataFrame.sort("salary").select("name","salary").show()
    personDataFrame.sort($"salary".desc).select("name","salary").show()
    personDataFrame.sort($"salary".desc,$"age".asc).select("name","salary").show()
    personDataFrame
      .repartition(5)
      .orderBy($"salary".desc,$"age".asc)
      .select("name","salary")
      .show()
    //局部排序
    print("----------局部排序-------")
    personDataFrame
      .repartition(5)
      .sortWithinPartitions($"salary".desc,$"age".asc)
      .select("name","salary")
      .show()
    //group by
    personDataFrame
    .groupBy("sex")
    .agg(
        "salary" ->"avg",
        "salary" ->"sum"
        //"salary" ->"max"
      ).show()
    personDataFrame
      .groupBy("sex")
      .agg(
    avg("salary").as("avg_salary"),
    min("salary").as("salary")
      )
    .show()

    println("----------join--------------------")
    personDataFrame.join(deptDataFrame).show()
    personDataFrame.join(deptDataFrame.toDF("col1","deptName"),$"deptNo"==="col1","inner").show()
    personDataFrame.join(deptDataFrame,"deptNo").show()
    personDataFrame.unpersist()
    deptDataFrame.unpersist()
  }
}
