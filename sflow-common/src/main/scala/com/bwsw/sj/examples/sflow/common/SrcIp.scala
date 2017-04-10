package com.bwsw.sj.examples.sflow.common

/**
  * @author Pavel Tomskikh
  */
case class SrcIp(srcIP: String, traffic: Int) extends Serializable

object SrcIp {
  def apply(tuple: (String, Int)) = new SrcIp(tuple._1, tuple._2)
}
