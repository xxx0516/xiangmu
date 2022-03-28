package com.zz.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object AppTags extends TagTrait {
  override def makeTags(args: Any*): Map[String, Int] = {

    // 设定返回值类型。
    var map = Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]

    // 接收广播变量的值。
    val broadcast: Map[String, String] = args(1).asInstanceOf[Map[String, String]]

    val appName: String = row.getAs[String]("appname")
    val appId: String = row.getAs[String]("appid")

    //  App 名称标签
    /*
    2)App 名称（标签格式：	）xxxx 为 App 名称，使用缓存文件   是不是要用redis  appname_dict 进行名称转换；APP 爱奇艺->1
3)渠道（标签格式： CNxxxx->1）xxxx 为渠道 ID(adplatformproviderid)
     */
    val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")

    if (StringUtils.isEmpty(appName)) {
      broadcast.contains("appId") match {
        case true => map += "APP" + broadcast.getOrElse("appId", "未知") -> 1
      }
    } else {
      map += "APP" + appName -> 1
    }


    // 渠道标签
    map+= "CN"+adplatformproviderid -> 1




map
  }







}
