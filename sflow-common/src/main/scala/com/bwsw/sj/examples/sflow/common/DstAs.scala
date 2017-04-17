package com.bwsw.sj.examples.sflow.common

/**
  * @author Pavel Tomskikh
  */
case class DstAs(dstAs: Int, traffic: Int) extends Serializable

object DstAs {
  def apply(tuple: (Int, Int)) = new DstAs(tuple._1, tuple._2)
}
