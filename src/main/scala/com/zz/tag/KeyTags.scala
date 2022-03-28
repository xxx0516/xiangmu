package com.zz.tag

import org.apache.spark.sql.Row

object KeyTags extends  TagTrait {
  override def makeTags(args: Any*): Map[String, Int] = {
    // 设定返回值类型。
    var map = Map[String,Int]()
    val row: Row = args(0).asInstanceOf[Row]

    // 停用词.
    val broadcast: Map[String, Int] = args(1).asInstanceOf[Map[String, Int]]
//关键字标签
    /*
    7)关键字（标签格式：Kxxx->1）xxx 为关键字，关键字个数不能少于 3 个字符，且不能
超过 8 个字符；关键字中如包含‘‘|’’，则分割成数组，转化成多个关键字标签
     */
    val keywords: String = row.getAs[String]("keywords")
    val ks: Array[String] = keywords.split("\\|")

    ks.filter(kw=>kw.length>=3 && kw.length<=8 && !broadcast.contains(kw))
      .foreach(kw=>map += "K"+kw ->1)

map

  }
}
