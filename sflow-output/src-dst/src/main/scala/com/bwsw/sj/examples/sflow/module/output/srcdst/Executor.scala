package com.bwsw.sj.examples.sflow.module.output.srcdst

import com.bwsw.sj.common.engine.core.entities.TStreamEnvelope
import com.bwsw.sj.common.engine.core.environment.OutputEnvironmentManager
import com.bwsw.sj.common.engine.core.output.OutputStreamingExecutor
import com.bwsw.sj.engine.core.output.types.jdbc.{IntegerField, JdbcEntityBuilder}
import com.bwsw.sj.examples.sflow.common.{JdbcFieldsNames, SrcDstAs}
import com.bwsw.sj.examples.sflow.module.output.srcdst.data.SrcDstData
import com.typesafe.scalalogging.Logger

/**
  * Created by diryavkin_dn on 13.01.17.
  */
class Executor(manager: OutputEnvironmentManager) extends OutputStreamingExecutor[SrcDstAs](manager) {

  private val logger = Logger(this.getClass)

  /**
    * Transform t-stream transaction to output entities
    *
    * @param envelope Input T-Stream envelope
    * @return List of output envelopes
    */
  override def onMessage(envelope: TStreamEnvelope[SrcDstAs]) = {
    logger.debug("Invoked onMessage.")
    envelope.data.map { srcDstAs =>
      logger.debug(s"SrcDstAs: $srcDstAs")
      new SrcDstData(
        srcDstAs.srcAs,
        srcDstAs.dstAs,
        srcDstAs.traffic)
    }
  }

  override def getOutputEntity = {
    new JdbcEntityBuilder()
      .field(new IntegerField(JdbcFieldsNames.srcAsField))
      .field(new IntegerField(JdbcFieldsNames.dstAsField))
      .field(new IntegerField(JdbcFieldsNames.trafficField))
      .build()
  }
}
