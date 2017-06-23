package com.bwsw.sj.examples.sflow.module.output.srcip.data

import com.bwsw.sj.common.engine.core.entities.OutputEnvelope
import com.bwsw.sj.examples.sflow.common.JdbcFieldsNames

/**
  * @author Pavel Tomskikh
  */
class SrcIpData(srcIp: String, traffic: Int) extends OutputEnvelope {
  override def getFieldsValue: Map[String, Any] = Map(
    JdbcFieldsNames.srcIpField -> srcIp,
    JdbcFieldsNames.trafficField -> traffic)
}
