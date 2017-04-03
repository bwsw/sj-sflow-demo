package com.bwsw.sj.examples.sflow.module.output.fallback

import com.bwsw.sj.engine.core.entities.TStreamEnvelope
import com.bwsw.sj.engine.core.environment.OutputEnvironmentManager
import com.bwsw.sj.engine.core.output.OutputStreamingExecutor
import com.bwsw.sj.engine.core.output.types.jdbc.{JavaStringField, JdbcEntityBuilder}
import com.bwsw.sj.examples.sflow.module.output.fallback.data.Fallback
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.slf4j.LoggerFactory

/**
  * @author Pavel Tomskikh
  */
class Executor(manager: OutputEnvironmentManager) extends OutputStreamingExecutor[GenericRecord](manager) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def onMessage(envelope: TStreamEnvelope[GenericRecord]) = {
    logger.debug("Invoked onMessage.")
    envelope.data.map { record =>
      val line = record.get("data").asInstanceOf[Utf8].toString
      logger.debug(s"Fallback line: $line.")
      new Fallback(line)
    }
  }

  override def getOutputEntity = {
    new JdbcEntityBuilder()
      .field(new JavaStringField("id"))
      .field(new JavaStringField("line"))
      .build()
  }
}
