package com.bwsw.sj.examples.sflow.module.process

import com.bwsw.common.ObjectSerializer
import com.bwsw.sj.common.utils.GeoIp
import com.bwsw.sj.engine.core.entities.TStreamEnvelope
import com.bwsw.sj.engine.core.environment.ModuleEnvironmentManager
import com.bwsw.sj.engine.core.windowed.{WindowRepository, WindowedStreamingExecutor}
import com.bwsw.sj.examples.sflow.common.SflowRecord
import com.bwsw.sj.examples.sflow.module.process.mapreduce.Generator
import org.apache.avro.generic.GenericData.Record

import scala.collection.mutable.ArrayBuffer


class Executor(manager: ModuleEnvironmentManager) extends WindowedStreamingExecutor[Record](manager) {
  private val objectSerializer = new ObjectSerializer()
  private val storage = ArrayBuffer[SflowRecord]()

  override def onWindow(windowRepository: WindowRepository): Unit = {
    val allWindows = windowRepository.getAll()

    val envelopes = allWindows.flatMap(_._2.batches).flatMap(_.envelopes).map(_.asInstanceOf[TStreamEnvelope[Record]])
    val sflowRecords = envelopes.flatMap(_.data.map { avroRecord =>
      val _srcIP = avroRecord.get(FieldsNames.srcIP).asInstanceOf[String]
      val _dstIP = avroRecord.get(FieldsNames.dstIP).asInstanceOf[String]
      SflowRecord(
        timestamp = avroRecord.get(FieldsNames.timestamp).asInstanceOf[String].toLong,
        name = avroRecord.get(FieldsNames.name).asInstanceOf[String],
        agentAddress = avroRecord.get(FieldsNames.agentAddress).asInstanceOf[String],
        inputPort = avroRecord.get(FieldsNames.inputPort).asInstanceOf[String].toInt,
        outputPort = avroRecord.get(FieldsNames.outputPort).asInstanceOf[String].toInt,
        srcMAC = avroRecord.get(FieldsNames.srcMAC).asInstanceOf[String],
        dstMAC = avroRecord.get(FieldsNames.dstMAC).asInstanceOf[String],
        ethernetType = avroRecord.get(FieldsNames.ethernetType).asInstanceOf[String],
        inVlan = avroRecord.get(FieldsNames.inVlan).asInstanceOf[String].toInt,
        outVlan = avroRecord.get(FieldsNames.outVlan).asInstanceOf[String].toInt,
        srcIP = _srcIP,
        dstIP = _dstIP,
        ipProtocol = avroRecord.get(FieldsNames.ipProtocol).asInstanceOf[String].toInt,
        ipTos = avroRecord.get(FieldsNames.ipTos).asInstanceOf[String],
        ipTtl = avroRecord.get(FieldsNames.ipTtl).asInstanceOf[String].toInt,
        udpSrcPort = avroRecord.get(FieldsNames.udpSrcPort).asInstanceOf[String].toInt,
        udpDstPort = avroRecord.get(FieldsNames.udpDstPort).asInstanceOf[String].toInt,
        tcpFlags = avroRecord.get(FieldsNames.tcpFlags).asInstanceOf[String],
        packetSize = avroRecord.get(FieldsNames.packetSize).asInstanceOf[String].toInt,
        ipSize = avroRecord.get(FieldsNames.ipSize).asInstanceOf[String].toInt,
        samplingRate = avroRecord.get(FieldsNames.samplingRate).asInstanceOf[String].toInt,
        srcAs = GeoIp.resolveAs(_srcIP),
        dstAs = GeoIp.resolveAs(_dstIP))
    })

    storage ++= sflowRecords
  }

  override def onEnter(): Unit = {
    val gen = new Generator()
    gen.putRecords(storage.asInstanceOf[Array[SflowRecord]])

    var output = manager.getRoundRobinOutput("SrcAsStream")
    output.put(objectSerializer.serialize(gen.SrcAsReduceResult()))

    output = manager.getRoundRobinOutput("DstAsStream")
    output.put(objectSerializer.serialize(gen.DstAsReduceResult()))

    output = manager.getRoundRobinOutput("SrcDstStream")
    output.put(objectSerializer.serialize(gen.SrcDstReduceResult()))

    output = manager.getRoundRobinOutput("SrcIpStream")
    output.put(objectSerializer.serialize(gen.SrcIpReduceResult()))

    output = manager.getRoundRobinOutput("DstIpStream")
    output.put(objectSerializer.serialize(gen.DstIpReduceResult()))

    gen.clear()
  }

}

object FieldsNames {
  val timestamp = "timestamp"
  val name = "name"
  val agentAddress = "agentAddress"
  val inputPort = "inputPort"
  val outputPort = "outputPort"
  val srcMAC = "srcMAC"
  val dstMAC = "dstMAC"
  val ethernetType = "ethernetType"
  val inVlan = "inVlan"
  val outVlan = "outVlan"
  val srcIP = "srcIP"
  val dstIP = "dstIP"
  val ipProtocol = "ipProtocol"
  val ipTos = "ipTos"
  val ipTtl = "ipTtl"
  val udpSrcPort = "udpSrcPort"
  val udpDstPort = "udpDstPort"
  val tcpFlags = "tcpFlags"
  val packetSize = "packetSize"
  val ipSize = "ipSize"
  val samplingRate = "samplingRate"
}
