package com.bwsw.sj.examples.sflow.common

/**
  * @author Pavel Tomskikh
  */
case class DstIp(dstIP: String, traffic: Int) extends Serializable

object DstIp {
  def apply(tuple: (String, Int)) = new DstIp(tuple._1, tuple._2)
}

