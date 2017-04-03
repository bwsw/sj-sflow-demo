package com.bwsw.sj.examples.sflow.module.output

import com.bwsw.sj.engine.core.entities.TStreamEnvelope
import com.bwsw.sj.engine.core.environment.OutputEnvironmentManager
import com.bwsw.sj.engine.core.output.OutputStreamingExecutor
import com.bwsw.sj.engine.core.output.types.jdbc.{IntegerField, JavaStringField, JdbcEntityBuilder}
import com.bwsw.sj.examples.sflow.common.OutputRecord
import com.bwsw.sj.examples.sflow.module.output.data.{FieldsNames, OutputData}

/**
  * Created by diryavkin_dn on 13.01.17.
  */
class Executor(manager: OutputEnvironmentManager) extends OutputStreamingExecutor[OutputRecord](manager) {

  /**
    * Transform t-stream transaction to output entities
    *
    * @param envelope Input T-Stream envelope
    * @return List of output envelopes
    */
  override def onMessage(envelope: TStreamEnvelope[OutputRecord]) = {
    envelope.data.map { outputRecord =>
      new OutputData(
        outputRecord.srcIp,
        outputRecord.srcAs,
        outputRecord.dstIp,
        outputRecord.dstAs,
        outputRecord.traffic)
    }
  }

  override def getOutputEntity = {
    new JdbcEntityBuilder()
      .field(new JavaStringField(FieldsNames.idField))
      .field(new JavaStringField(FieldsNames.srcIpField))
      .field(new IntegerField(FieldsNames.srcAsField))
      .field(new JavaStringField(FieldsNames.dstIpField))
      .field(new IntegerField(FieldsNames.dstAsField))
      .field(new IntegerField(FieldsNames.trafficField))
      .build()
  }
}
