package com.bwsw.sj.examples.sflow.module.output.data

/**
  * Created by diryavkin_dn on 17.01.17.
  */
class SrcDstData(srcAs: Int, dstAs: Int, traffic: Int) extends DataEntity(traffic) {
  override def getExtraFields = Map("src_as" -> srcAs, "dst_as" -> dstAs)
}



