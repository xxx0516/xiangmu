package com.zz.tag


/*
*特性
 */

//Trait是指可以混入或融入一个类层次结构的行为
trait TagTrait {

  def makeTags(args:Any*):Map[String,Int]


}
