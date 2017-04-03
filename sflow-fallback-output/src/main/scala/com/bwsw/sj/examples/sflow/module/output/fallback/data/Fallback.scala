package com.bwsw.sj.examples.sflow.module.output.fallback.data

import com.bwsw.sj.engine.core.entities.OutputEnvelope

/**
  * @author Pavel Tomskikh
  */
class Fallback(line: String) extends OutputEnvelope {
  var id: String = java.util.UUID.randomUUID.toString

  override def getFieldsValue = Map("id" -> id, "line" -> line)
}
