package com.bwsw.sj.examples.sflow.common

/**
  * @author Pavel Tomskikh
  */
class SrcDstAs(val srcAs: Int, val dstAs: Int, val traffic: Int)

object SrcDstAs {
  def apply(s: SflowRecord) = new SrcDstAs(s.srcAs, s.dstAs, s.getTraffic)
}

