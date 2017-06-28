package com.bwsw.sj.examples.sflow.module.process

import com.bwsw.sj.common.dal.model.instance.BatchInstanceDomain
import com.bwsw.sj.common.dal.model.stream.StreamDomain
import com.bwsw.sj.common.dal.repository.Repository
import com.bwsw.sj.common.engine.core.batch.{BatchCollector, BatchStreamingPerformanceMetrics}
import com.bwsw.sj.common.engine.core.entities.Envelope

import scala.collection.mutable

/**
  * @author Pavel Tomskikh
  */
class SflowBatchCollector(
    instance: BatchInstanceDomain,
    performanceMetrics: BatchStreamingPerformanceMetrics,
    streamRepository: Repository[StreamDomain])
  extends BatchCollector(instance, performanceMetrics, streamRepository) {

  private val countOfEnvelopesPerStream = mutable.Map(instance.getInputsWithoutStreamMode.map(x => (x, 0)): _*)

  override def prepareForNextCollecting(streamName: String) =
    resetCounter(streamName)

  override def getBatchesToCollect() =
    countOfEnvelopesPerStream.filter(x => x._2 > 0).keys.toSeq

  override def afterEnvelopeReceive(envelope: Envelope) =
    increaseCounter(envelope)

  private def increaseCounter(envelope: Envelope) =
    countOfEnvelopesPerStream(envelope.stream) += 1

  private def resetCounter(streamName: String) =
    countOfEnvelopesPerStream(streamName) = 0
}
