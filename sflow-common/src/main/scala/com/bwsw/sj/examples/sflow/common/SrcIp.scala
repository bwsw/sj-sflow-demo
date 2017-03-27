package com.bwsw.sj.examples.sflow.common

/**
  * @author Pavel Tomskikh
  */
class SrcIp(val srcIP: String, val traffic: Int)

object SrcIp {
  def apply(s: SflowRecord) = new SrcIp(s.srcIP, s.getTraffic)
}