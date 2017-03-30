package com.bwsw.sj.examples.sflow.module.output

import com.bwsw.sj.engine.core.entities.TStreamEnvelope
import com.bwsw.sj.engine.core.environment.OutputEnvironmentManager
import com.bwsw.sj.engine.core.output.OutputStreamingExecutor
import com.bwsw.sj.engine.core.output.types.jdbc.{JavaStringField, JdbcEntityBuilder}
import com.bwsw.sj.examples.sflow.common._
import com.bwsw.sj.examples.sflow.module.output.data._

/**
  * Created by diryavkin_dn on 13.01.17.
  */
class Executor(manager: OutputEnvironmentManager) extends OutputStreamingExecutor[SrcDstAs](manager) {

  /**
    * Transform t-stream transaction to output entities
    *
    * @param envelope Input T-Stream envelope
    * @return List of output envelopes
    */
  override def onMessage(envelope: TStreamEnvelope[SrcDstAs]) = {
    envelope.data.map { srcDstAs =>
      new SrcDstData(srcDstAs.srcAs, srcDstAs.dstAs, srcDstAs.traffic)
    }
  }

  override def getOutputEntity = {
    new JdbcEntityBuilder()
      .field(new JavaStringField("id"))
      .field(new JavaStringField("src_as"))
      .field(new JavaStringField("dst_as"))
      .field(new JavaStringField("traffic"))
      .build()
  }
}
