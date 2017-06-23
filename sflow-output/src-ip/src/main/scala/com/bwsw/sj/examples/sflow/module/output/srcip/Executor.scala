package com.bwsw.sj.examples.sflow.module.output.srcip

import com.bwsw.sj.common.engine.core.entities.TStreamEnvelope
import com.bwsw.sj.common.engine.core.environment.OutputEnvironmentManager
import com.bwsw.sj.common.engine.core.output.OutputStreamingExecutor
import com.bwsw.sj.engine.core.output.types.jdbc.{IntegerField, JavaStringField, JdbcEntityBuilder}
import com.bwsw.sj.examples.sflow.common.{JdbcFieldsNames, SrcIp}
import com.bwsw.sj.examples.sflow.module.output.srcip.data.SrcIpData
import org.slf4j.LoggerFactory

/**
  * Created by diryavkin_dn on 13.01.17.
  */
class Executor(manager: OutputEnvironmentManager) extends OutputStreamingExecutor[SrcIp](manager) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Transform t-stream transaction to output entities
    *
    * @param envelope Input T-Stream envelope
    * @return List of output envelopes
    */
  override def onMessage(envelope: TStreamEnvelope[SrcIp]) = {
    logger.debug("Invoked onMessage.")
    envelope.data.map { srcIp =>
      logger.debug(s"SrcIp: $srcIp")
      new SrcIpData(
        srcIp.srcIP,
        srcIp.traffic)
    }
  }

  override def getOutputEntity = {
    new JdbcEntityBuilder()
      .field(new JavaStringField(JdbcFieldsNames.srcIpField))
      .field(new IntegerField(JdbcFieldsNames.trafficField))
      .build()
  }
}
