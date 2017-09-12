package com.bwsw.sj.examples.sflow.module.process

import com.bwsw.sj.common.dal.model.instance.BatchInstanceDomain
import com.bwsw.sj.common.dal.model.stream.StreamDomain
import com.bwsw.sj.common.engine.core.batch.{BatchCollector, BatchStreamingPerformanceMetrics}
import com.bwsw.sj.common.engine.core.entities.Envelope

import scala.collection.mutable

/**
  * @author Pavel Tomskikh
  */
class SflowBatchCollector(instance: BatchInstanceDomain,
                          performanceMetrics: BatchStreamingPerformanceMetrics,
                          inputs: Array[StreamDomain])
  extends BatchCollector(instance, performanceMetrics, inputs) {

  private val countOfEnvelopesPerStream = mutable.Map(instance.getInputsWithoutStreamMode.map(x => (x, 0)): _*)

  override def prepareForNextCollecting(streamName: String): Unit =
    countOfEnvelopesPerStream(streamName) = 0

  override def getBatchesToCollect(): Seq[String] =
    countOfEnvelopesPerStream.filter(_._2 > 0).keys.toSeq

  override def afterEnvelopeReceive(envelope: Envelope): Unit =
    countOfEnvelopesPerStream(envelope.stream) += 1
}
