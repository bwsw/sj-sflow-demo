package com.bwsw.sj.examples.sflow.common

/**
  * @author Pavel Tomskikh
  */
class DstIp(val dstIP: String, val traffic: Int)

object DstIp {
  def apply(s: SflowRecord): DstIp = new DstIp(s.dstIP, s.getTraffic)
}