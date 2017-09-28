package com.bwsw.sj.examples.sflow.module.output.fallback

import com.bwsw.common.AvroSerializer
import com.bwsw.sj.common.engine.core.entities.TStreamEnvelope
import com.bwsw.sj.common.engine.core.environment.OutputEnvironmentManager
import com.bwsw.sj.common.engine.core.output.OutputStreamingExecutor
import com.bwsw.sj.engine.core.output.types.jdbc.{JavaStringField, JdbcEntityBuilder}
import com.bwsw.sj.examples.sflow.module.output.fallback.data.Fallback
import com.typesafe.scalalogging.Logger
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

/**
  * @author Pavel Tomskikh
  */
class Executor(manager: OutputEnvironmentManager) extends OutputStreamingExecutor[GenericRecord](manager) {

  private val logger = Logger(this.getClass)
  private val dataField = "data"
  private val schema = SchemaBuilder.record("fallback").fields()
    .name(dataField).`type`().stringType().noDefault()
    .endRecord()
  private val avroSerializer = new AvroSerializer

  override def onMessage(envelope: TStreamEnvelope[GenericRecord]) = {
    logger.debug("Invoked onMessage.")
    envelope.data.map { record =>
      val line = record.get(dataField).asInstanceOf[Utf8].toString
      logger.debug(s"Fallback line: $line.")
      new Fallback(line)
    }
  }

  override def getOutputEntity = {
    new JdbcEntityBuilder()
      .field(new JavaStringField("line"))
      .build()
  }

  override def deserialize(bytes: Array[Byte]): GenericRecord = avroSerializer.deserialize(bytes, schema)
}
