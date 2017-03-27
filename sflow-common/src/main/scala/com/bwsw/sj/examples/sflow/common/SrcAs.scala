package com.bwsw.sj.examples.sflow.common

/**
  * @author Pavel Tomskikh
  */
class SrcAs(val srcAs: Int, val traffic: Int)

object SrcAs {
  def apply(s: SflowRecord) = new DstAs(s.srcAs, s.getTraffic)
}
