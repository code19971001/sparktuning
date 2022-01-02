package com.atguigu.sparktuning.utils

import java.util.Random
import com.atguigu.sparktuning.bean.{School, Student}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object InitUtil {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("InitData")
      .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
    val sparkSession: SparkSession = initSparkSession(sparkConf)
    initHiveTable(sparkSession)
    //initBucketTable(sparkSession)
    saveData(sparkSession)
  }

  def initSparkSession(sparkConf: SparkConf): SparkSession = {
    //主要用于kerberos的认证
    try {

      //等同于把krb5.conf放在$JAVA_HOME\jre\lib\security，一般写代码即可
      System.setProperty("java.security.krb5.conf", "C:\\ProgramData\\MIT\\Kerberos5\\krb5.ini")
      //下面的conf可以注释掉是因为在core-site.xml里有相关的配置，如果没有相关的配置，则下面的代码是必须的
      //      val conf = new Configuration
      //      conf.set("hadoop.security.authentication", "kerberos")
      //      UserGroupInformation.setConfiguration(conf)
      UserGroupInformation.loginUserFromKeytab("code1997@CODE1997.COM", "C:\\ProgramData\\MIT\\Kerberos5\\code1997.keytab")
      println(UserGroupInformation.getCurrentUser, UserGroupInformation.getLoginUser)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    System.setProperty("HADOOP_USER_NAME", "code1997")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    // TODO 改成自己的hadoop的nameNode的url，在core-site文件中
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://hadoop02:8020")
    sparkSession
  }

  def initHiveTable(sparkSession: SparkSession): Unit = {
    sparkSession.read.json("/origin_data/sparktuning/coursepay.log")
      .write.partitionBy("dt", "dn")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.course_pay")

    sparkSession.read.json("/origin_data/sparktuning/salecourse.log")
      .write.partitionBy("dt", "dn")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.sale_course")

    sparkSession.read.json("/origin_data/sparktuning/courseshoppingcart.log")
      .write.partitionBy("dt", "dn")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.course_shopping_cart")

  }

  def initBucketTable(sparkSession: SparkSession): Unit = {
    sparkSession.read.json("/origin_data/sparktuning/coursepay.log")
      .write.partitionBy("dt", "dn")
      .format("parquet")
      .bucketBy(5, "orderid")
      .sortBy("orderid")
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.course_pay_cluster")
    sparkSession.read.json("/origin_data/sparktuning/courseshoppingcart.log")
      .write.partitionBy("dt", "dn")
      .bucketBy(5, "orderid")
      .format("parquet")
      .sortBy("orderid")
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.course_shopping_cart_cluster")
  }

  def saveData(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    sparkSession.range(1000000).mapPartitions(partitions => {
      val random = new Random()
      partitions.map(item => Student(item, "name" + item, random.nextInt(100), random.nextInt(100)))
    }).write.partitionBy("partition")
      .mode(SaveMode.Append)
      .saveAsTable("sparktuning.test_student")

    sparkSession.range(1000000).mapPartitions(partitions => {
      val random = new Random()
      partitions.map(item => School(item, "school" + item, random.nextInt(100)))
    }).write.partitionBy("partition")
      .mode(SaveMode.Append)
      .saveAsTable("sparktuning.test_school")
  }

}
