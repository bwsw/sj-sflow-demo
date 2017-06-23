package com.bwsw.sj.examples.sflow.module.output.srcdst.data

import com.bwsw.sj.common.engine.core.entities.OutputEnvelope
import com.bwsw.sj.examples.sflow.common.JdbcFieldsNames

/**
  * @author Pavel Tomskikh
  */
class SrcDstData(srcAs: Int, dstAs: Int, traffic: Int) extends OutputEnvelope {
  override def getFieldsValue: Map[String, Any] = Map(
    JdbcFieldsNames.srcAsField -> srcAs,
    JdbcFieldsNames.dstAsField -> dstAs,
    JdbcFieldsNames.trafficField -> traffic)
}
