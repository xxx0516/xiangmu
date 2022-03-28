package com.zz.analyse

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object ProCityAnalyseSaveMysql {

  def main(args: Array[String]): Unit = {
    // 判断参数是否正确。
    if (args.length != 1) {
      println(
        """
          |缺少参数
          |inputpath  outputpath
          |""".stripMargin)
      sys.exit()
    }

    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[1]").getOrCreate()

    var sc = spark.sparkContext

    import spark.implicits._

    // 接收参数
    var Array(inputPath) = args
    val df: DataFrame = spark.read.parquet(inputPath)
    df.createTempView("log")


    // 编写sql语句
    var sql = "select provincename,cityname,count(*) as pccount from log group by provincename,cityname"

    val resDF: DataFrame = spark.sql(sql)

    val load: Config = ConfigFactory.load()

    val pro: Properties = new Properties()
    pro.setProperty("user",load.getString("jdbc.user"))
    pro.setProperty("driver",load.getString("jdbc.driver"))
    pro.setProperty("password",load.getString("jdbc.password"))



    resDF.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),pro)

    //关闭对象
    spark.stop()


  }
}