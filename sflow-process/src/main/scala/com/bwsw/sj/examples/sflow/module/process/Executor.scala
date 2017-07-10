package com.bwsw.sj.examples.sflow.module.process

import java.io.File

import com.bwsw.common.{AvroSerializer, JsonSerializer}
import com.bwsw.sj.common.engine.core.batch.{BatchStreamingExecutor, WindowRepository}
import com.bwsw.sj.common.engine.core.entities.TStreamEnvelope
import com.bwsw.sj.common.engine.core.environment.ModuleEnvironmentManager
import com.bwsw.sj.common.engine.core.state.StateStorage
import com.bwsw.sj.common.utils.GeoIp
import com.bwsw.sj.examples.sflow.common._
import com.bwsw.sj.examples.sflow.module.process.OptionsLiterals._
import com.bwsw.sj.examples.sflow.module.process.mapreduce.Generator
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.avro.{Schema, SchemaBuilder}
import org.slf4j.LoggerFactory

class Executor(manager: ModuleEnvironmentManager) extends BatchStreamingExecutor[Record](manager) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val state: StateStorage = manager.getState
  private val stateField = "sflowRecords"
  private val schema = createSchema
  private val avroSerializer = new AvroSerializer
  private val jsonSerializer = new JsonSerializer(ignoreUnknown = true)
  private val geoIp = createGeoIp

  // val dstAsStream = manager.getRoundRobinOutput("dst-as-stream")
  // val dstIpStream = manager.getRoundRobinOutput("dst-ip-stream")
  // val srcAsStream = manager.getRoundRobinOutput("src-as-stream")
  val srcIpStream = manager.getRoundRobinOutput(StreamNames.srcIpStream)
  val srcDstStream = manager.getRoundRobinOutput(StreamNames.srcDstStream)

  val gen = new Generator()

  override def onInit() = {
    logger.debug("Invoked onInit.")
    if (!state.isExist(stateField) || !state.get(stateField).isInstanceOf[Iterable[_]])
      state.set(stateField, Iterable[SflowRecord]())
  }

  override def onWindow(windowRepository: WindowRepository): Unit = {
    logger.debug("Invoked onWindow.")
    val storage = state.get(stateField).asInstanceOf[Iterable[SflowRecord]]
    val allWindows = windowRepository.getAll()

    val envelopes = allWindows.flatMap(_._2.batches).flatMap(_.envelopes).map(_.asInstanceOf[TStreamEnvelope[Record]])
    val sflowRecords = envelopes.flatMap(_.data.map { avroRecord =>
      try {
        val _srcIP = avroRecord.get(FieldsNames.srcIP).asInstanceOf[Utf8].toString
        val _dstIP = avroRecord.get(FieldsNames.dstIP).asInstanceOf[Utf8].toString
        SflowRecord(
          timestamp = avroRecord.get(FieldsNames.timestamp).asInstanceOf[Utf8].toString.toLong,
          name = avroRecord.get(FieldsNames.name).asInstanceOf[Utf8].toString,
          agentAddress = avroRecord.get(FieldsNames.agentAddress).asInstanceOf[Utf8].toString,
          inputPort = avroRecord.get(FieldsNames.inputPort).asInstanceOf[Utf8].toString.toInt,
          outputPort = avroRecord.get(FieldsNames.outputPort).asInstanceOf[Utf8].toString.toInt,
          srcMAC = avroRecord.get(FieldsNames.srcMAC).asInstanceOf[Utf8].toString,
          dstMAC = avroRecord.get(FieldsNames.dstMAC).asInstanceOf[Utf8].toString,
          ethernetType = avroRecord.get(FieldsNames.ethernetType).asInstanceOf[Utf8].toString,
          inVlan = avroRecord.get(FieldsNames.inVlan).asInstanceOf[Utf8].toString.toInt,
          outVlan = avroRecord.get(FieldsNames.outVlan).asInstanceOf[Utf8].toString.toInt,
          srcIP = _srcIP,
          dstIP = _dstIP,
          ipProtocol = avroRecord.get(FieldsNames.ipProtocol).asInstanceOf[Utf8].toString.toInt,
          ipTos = avroRecord.get(FieldsNames.ipTos).asInstanceOf[Utf8].toString,
          ipTtl = avroRecord.get(FieldsNames.ipTtl).asInstanceOf[Utf8].toString.toInt,
          udpSrcPort = avroRecord.get(FieldsNames.udpSrcPort).asInstanceOf[Utf8].toString.toInt,
          udpDstPort = avroRecord.get(FieldsNames.udpDstPort).asInstanceOf[Utf8].toString.toInt,
          tcpFlags = avroRecord.get(FieldsNames.tcpFlags).asInstanceOf[Utf8].toString,
          packetSize = avroRecord.get(FieldsNames.packetSize).asInstanceOf[Utf8].toString.toInt,
          ipSize = avroRecord.get(FieldsNames.ipSize).asInstanceOf[Utf8].toString.toInt,
          samplingRate = avroRecord.get(FieldsNames.samplingRate).asInstanceOf[Utf8].toString.toInt,
          srcAs = tryResolve(_srcIP),
          dstAs = tryResolve(_dstIP))
      } catch {
        case _: Throwable =>
          logger.debug(s"Incorrect record.")
          null
      }
    }).filter(_ != null)

    logger.debug(s"Sflow records: ${sflowRecords.mkString(", ")}.")

    state.set(stateField, storage ++ sflowRecords)
  }

  override def onEnter(): Unit = {
    logger.debug("Invoked onEnter.")
    val sflowRecords = state.get(stateField).asInstanceOf[Iterable[SflowRecord]]
    gen.putRecords(sflowRecords)
  }

  override def onLeaderEnter(): Unit = {
    // gen.DstAsReduceResult().foreach(tuple => dstAsStream.put(DstAs(tuple)))
    // gen.DstIpReduceResult().foreach(tuple => dstIpStream.put(DstIp(tuple)))
    // gen.SrcAsReduceResult().foreach(tuple => srcAsStream.put(SrcAs(tuple)))
    gen.SrcIpReduceResult().foreach(tuple => srcIpStream.put(SrcIp(tuple)))
    gen.SrcDstReduceResult().foreach(tuple => srcDstStream.put(SrcDstAs(tuple)))
  }

  override def onBeforeCheckpoint(): Unit = {
    gen.clear()
    state.set(stateField, Iterable[SflowRecord]())
  }

  override def deserialize(bytes: Array[Byte]): GenericRecord =
    avroSerializer.deserialize(bytes, schema)

  private def tryResolve(ip: String) = {
    try {
      geoIp.resolveAs(ip)
    } catch {
      case _: Throwable => 0
    }
  }

  private def createSchema: Schema = {
    SchemaBuilder.record("csv").fields()
      .name(FieldsNames.timestamp).`type`().stringType().noDefault()
      .name(FieldsNames.name).`type`().stringType().noDefault()
      .name(FieldsNames.agentAddress).`type`().stringType().noDefault()
      .name(FieldsNames.inputPort).`type`().stringType().noDefault()
      .name(FieldsNames.outputPort).`type`().stringType().noDefault()
      .name(FieldsNames.srcMAC).`type`().stringType().noDefault()
      .name(FieldsNames.dstMAC).`type`().stringType().noDefault()
      .name(FieldsNames.ethernetType).`type`().stringType().noDefault()
      .name(FieldsNames.inVlan).`type`().stringType().noDefault()
      .name(FieldsNames.outVlan).`type`().stringType().noDefault()
      .name(FieldsNames.srcIP).`type`().stringType().noDefault()
      .name(FieldsNames.dstIP).`type`().stringType().noDefault()
      .name(FieldsNames.ipProtocol).`type`().stringType().noDefault()
      .name(FieldsNames.ipTos).`type`().stringType().noDefault()
      .name(FieldsNames.ipTtl).`type`().stringType().noDefault()
      .name(FieldsNames.udpSrcPort).`type`().stringType().noDefault()
      .name(FieldsNames.udpDstPort).`type`().stringType().noDefault()
      .name(FieldsNames.tcpFlags).`type`().stringType().noDefault()
      .name(FieldsNames.packetSize).`type`().stringType().noDefault()
      .name(FieldsNames.ipSize).`type`().stringType().noDefault()
      .name(FieldsNames.samplingRate).`type`().stringType().noDefault()
      .endRecord()
  }

  private def createGeoIp: GeoIp = {
    val optionsMap = jsonSerializer.deserialize[Map[String, String]](manager.options)

    def getFile(fieldName: String): Option[File] = {
      optionsMap.get(fieldName).filter(_.length > 0)
        .map(name => manager.getFileStorage.get(name, name))
    }

    val ipv4DatabaseFile = getFile(ipv4DatField)
    val ipv6DatabaseFile = getFile(ipv6DatField)

    new GeoIp(ipv4DatabaseFile, ipv6DatabaseFile)
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

object StreamNames {
  val srcIpStream = "src-ip-stream"
  val srcDstStream = "src-dst-stream"
}
