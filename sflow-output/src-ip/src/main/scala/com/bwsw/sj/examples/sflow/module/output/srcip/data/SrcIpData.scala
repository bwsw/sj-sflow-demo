package com.bwsw.sj.examples.sflow.module.output.srcip.data

import com.bwsw.sj.engine.core.entities.OutputEnvelope
import com.bwsw.sj.examples.sflow.common.JdbcFieldsNames

/**
  * @author Pavel Tomskikh
  */
class SrcIpData(srcIp: String, traffic: Int) extends OutputEnvelope {
  var id: String = java.util.UUID.randomUUID.toString

  override def getFieldsValue: Map[String, Any] = Map(
    JdbcFieldsNames.idField -> id,
    JdbcFieldsNames.srcIpField -> srcIp,
    JdbcFieldsNames.trafficField -> traffic)
}
