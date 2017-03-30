package com.bwsw.sj.examples.sflow.module.output.data

import com.bwsw.sj.engine.core.entities.OutputEnvelope

/**
  * @author Pavel Tomskikh
  */
abstract class DataEntity(traffic: Int) extends OutputEnvelope {
  var id: String = java.util.UUID.randomUUID.toString

  def getExtraFields: Map[String, Any]

  override def getFieldsValue = Map("id" -> id, "traffic" -> traffic) ++ getExtraFields
}
