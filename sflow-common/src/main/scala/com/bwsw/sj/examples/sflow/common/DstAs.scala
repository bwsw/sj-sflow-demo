package com.bwsw.sj.examples.sflow.common

/**
  * @author Pavel Tomskikh
  */
class DstAs(val dstAs: Int, val traffic: Int)

object DstAs {
  def apply(s: SflowRecord) = new DstAs(s.dstAs, s.getTraffic)
}
