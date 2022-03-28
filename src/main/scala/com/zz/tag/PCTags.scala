package com.zz.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object PCTags extends  TagTrait {
  override def makeTags(args: Any*): Map[String, Int] ={

    // 设定返回值类型。
    var map = Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]


/*
8)地域标签（省标签格式：ZPxxx->1, 地市标签格式: ZCxxx->1）xxx 为省或市名称
 */

    val provinceName: String = row.getAs[String]("provincename")
    val cityName: String = row.getAs[String]("cityname")
    if(StringUtils.isNotEmpty("provinceName")) map += "ZP"+provinceName->1
    if(StringUtils.isNotEmpty("cityname")) map += "ZP"+cityName->1



    map
  }
}
