package com.bwsw.sj.examples.sflow.module.output.fallback.data

import com.bwsw.sj.common.engine.core.entities.OutputEnvelope

/**
  * @author Pavel Tomskikh
  */
class Fallback(line: String) extends OutputEnvelope {
  override def getFieldsValue = Map("line" -> line)
}
