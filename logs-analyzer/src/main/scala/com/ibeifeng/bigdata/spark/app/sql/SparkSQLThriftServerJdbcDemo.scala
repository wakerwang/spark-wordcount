package com.ibeifeng.bigdata.spark.app.sql

import java.sql.DriverManager

/**
 * Created by wanglonglong on 2018/6/1.
 */
object SparkSQLThriftServerJdbcDemo {
  def main(args: Array[String]) {
    //添加Driver
    val driver="org.apache.hive.jdbc.HiveDriver"
    Class.forName(driver)
    //创建连接
    val (url,user,password)=("jdbc:hive2://bigdata.wll01.com:10000","hap","123")
    val conn=DriverManager.getConnection(url,user,password)
    //执行sql
    val sql="select * from db_emp.emp a join db_emp.dept b on a.deptno = b.deptno"
    val pstmt=conn.prepareStatement(sql)
    val rs=pstmt.executeQuery()
    while (rs.next()){
      println(rs.getString("ename")+":"+rs.getString("sal"))
    }
    rs.close()
    pstmt.close()

    println("==================================")
    val sql2="select AVG(sal) as avg_sal,deptno from db_emp.emp group by deptno having avg_sal > ?"
    val pstmt2=conn.prepareStatement(sql2)
    pstmt2.setInt(1,2000)

    val rs2=pstmt2.executeQuery()
    while (rs2.next()){
      println(rs2.getInt("deptno")+":"+rs2.getDouble("avg_sal"))
    }
    //关闭连接
    rs2.close()
    pstmt2.close()
  }
}
