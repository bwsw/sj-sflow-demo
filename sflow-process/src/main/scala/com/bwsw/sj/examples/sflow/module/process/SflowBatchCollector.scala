package com.bwsw.sj.examples.sflow.module.process

import com.bwsw.sj.common.DAL.model.module.WindowedInstance
import com.bwsw.sj.engine.core.entities.Envelope
import com.bwsw.sj.engine.core.windowed.{BatchCollector, WindowedStreamingPerformanceMetrics}

import scala.collection.mutable

/**
  *
  * @author Pavel Tomskikh
  */
class SflowBatchCollector(
    instance: WindowedInstance,
    performanceMetrics: WindowedStreamingPerformanceMetrics)
  extends BatchCollector(instance, performanceMetrics) {

  val a = instance.getInputsWithoutStreamMode()

  private val countOfEnvelopesPerStream = mutable.Map(instance.getInputsWithoutStreamMode().map(x => (x, 0)): _*)

  override def prepareForNextCollecting(streamName: String) = resetCounter(streamName)

  override def getBatchesToCollect() =
    countOfEnvelopesPerStream.filter(x => x._2 > 0).keys.toSeq

  override def afterReceivingEnvelope(envelope: Envelope) = increaseCounter(envelope)

  private def increaseCounter(envelope: Envelope) =
    countOfEnvelopesPerStream(envelope.stream) += 1

  private def resetCounter(streamName: String) =
    countOfEnvelopesPerStream(streamName) = 0

}
