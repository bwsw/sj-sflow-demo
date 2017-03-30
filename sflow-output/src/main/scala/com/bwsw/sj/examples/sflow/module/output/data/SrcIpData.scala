package com.bwsw.sj.examples.sflow.module.output.data

/**
  * Created by diryavkin_dn on 17.01.17.
  */
class SrcIpData(srcIp: String, traffic: Int) extends DataEntity(traffic) {
  override def getExtraFields = Map ("src_ip" -> srcIp)
}

