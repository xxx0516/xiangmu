package com.zz.tag

import org.apache.spark.sql.Row

object DriverTags extends TagTrait {
  override def makeTags(args: Any*): Map[String, Int] = {
    // 设定返回值类型。
    var map = Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]

    // 操作系统类型。
    val client: Int = row.getAs[Int]("client")
    // 联网方式类型。
    val networkmannername: String = row.getAs[String]("networkmannername")
    // 设备运行商名称.
    val ispname: String = row.getAs[String]("ispname")


    /**
     * 设备操作系统
1 Android D00010001
2 IOS D00010002
3 WinPhone D00010003
_ 其 他 D00010004

设 备 联 网 方式WIFI D00020001 4G D00020002
3G D00020003
2G D00020004
_   D00020005

设备运营商方式   模式匹配。
移 动 D00030001 联 通 D00030002 电 信 D00030003
_ D00030004
     */

    client match {
      case 1 => map += "D00010001" -> 1
      case 2 => map += "D00010002" -> 1
      case 3 => map += "D00010003" -> 1
      case _ => map += "D00010004" -> 1
    }

    networkmannername match {
      case "WIFI" => map += "D00020001 " -> 1
      case "4G" => map += "D00020002" -> 1
      case "3G" => map += "D00020003" -> 1
      case "2G" => map += "D00020004" -> 1
      case _ => map += "D00020005" -> 1
    }

    ispname match{
      case "移动" => map += "D00030001" -> 1
      case "联通" => map += "D00030002" -> 1
      case "电信" => map += "D00030003" -> 1
      case _  => map += "D00030004" -> 1
    }
    map
  }
}
