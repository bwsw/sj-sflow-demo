package com.bwsw.sj.examples.sflow.module.output.data

/**
  * Created by diryavkin_dn on 16.01.17.
  */

class SrcAsData(srcAs: Int, traffic: Int) extends DataEntity(traffic) {
  override def getExtraFields = Map("src_as" -> srcAs)
}

