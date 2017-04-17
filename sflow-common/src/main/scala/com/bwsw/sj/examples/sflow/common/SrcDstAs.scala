package com.bwsw.sj.examples.sflow.common

/**
  * @author Pavel Tomskikh
  */
case class SrcDstAs(srcAs: Int, dstAs: Int, traffic: Int) extends Serializable

object SrcDstAs {
  def apply(tuple: ((Int, Int), Int)) = new SrcDstAs(tuple._1._1, tuple._1._1, tuple._2)
}
