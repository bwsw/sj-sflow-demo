package com.bwsw.sj.examples.sflow.module.output.data

import com.bwsw.sj.engine.core.entities.OutputEnvelope

/**
  * @author Pavel Tomskikh
  */
class OutputData(srcIp: String, srcAs: Int, dstIp: String, dstAs: Int, traffic: Int) extends OutputEnvelope {
  var id: String = java.util.UUID.randomUUID.toString

  override def getFieldsValue: Map[String, Any] = Map(
    FieldsNames.idField -> id,
    FieldsNames.srcIpField -> srcIp,
    FieldsNames.srcAsField -> srcAs,
    FieldsNames.dstIpField -> dstIp,
    FieldsNames.dstAsField -> dstAs,
    FieldsNames.trafficField -> traffic)
}

object FieldsNames {
  val idField = "id"
  val srcIpField = "src_ip"
  val srcAsField = "src_as"
  val dstIpField = "dst_ip"
  val dstAsField = "dst_as"
  val trafficField = "traffic"
}



