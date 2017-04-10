package com.bwsw.sj.examples.sflow.module.output.srcdst.data

import com.bwsw.sj.engine.core.entities.OutputEnvelope
import com.bwsw.sj.examples.sflow.common.JdbcFieldsNames

/**
  * @author Pavel Tomskikh
  */
class SrcDstData(srcAs: Int, dstAs: Int, traffic: Int) extends OutputEnvelope {
  var id: String = java.util.UUID.randomUUID.toString

  override def getFieldsValue: Map[String, Any] = Map(
    JdbcFieldsNames.idField -> id,
    JdbcFieldsNames.srcAsField -> srcAs,
    JdbcFieldsNames.dstAsField -> dstAs,
    JdbcFieldsNames.trafficField -> traffic)
}
