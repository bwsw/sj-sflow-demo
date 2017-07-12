/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.bwsw.sj.examples.sflow.module.process

import java.io.File

import com.bwsw.common.JsonSerializer
import com.bwsw.common.file.utils.FileStorage
import com.bwsw.sj.common.dal.model.instance.BatchInstanceDomain
import com.bwsw.sj.common.dal.model.service.TStreamServiceDomain
import com.bwsw.sj.common.dal.model.stream.TStreamStreamDomain
import com.bwsw.sj.common.engine.core.batch.BatchStreamingPerformanceMetrics
import com.bwsw.sj.common.engine.core.entities.TStreamEnvelope
import com.bwsw.sj.common.engine.core.state.{RAMStateService, StateSaver, StateStorage}
import com.bwsw.sj.common.utils.GeoIp
import com.bwsw.sj.engine.core.simulation.SimulatorConstants
import com.bwsw.sj.engine.core.simulation.batch.{BatchEngineSimulator, BatchSimulationResult}
import com.bwsw.sj.engine.core.simulation.state._
import com.bwsw.sj.examples.sflow.common.{SflowRecord, SrcDstAs, SrcIp}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.util.Utf8
import org.apache.avro.{Schema, SchemaBuilder}
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Try

/**
  * Tests for [[Executor]]
  *
  * @author Pavel Tomskikh
  */
class ExecutorTests extends FlatSpec with Matchers with MockitoSugar {
  val stateSaver = mock[StateSaver]
  val stateLoader = new StateLoaderMock
  val stateService = new RAMStateService(stateSaver, stateLoader)
  val stateStorage = new StateStorage(stateService)

  val stateField = "sflowRecords"
  val initialState = Map(stateField -> Iterable[SflowRecord]())

  val geoIpDatabaseFilename = "GeoIPASNum.dat"
  val geoIpDatabase = new File(getClass.getResource("/" + geoIpDatabaseFilename).toURI)
  val geoIp = new GeoIp(Option(geoIpDatabase))
  val fileStorage = mock[FileStorage]
  when(fileStorage.get(geoIpDatabaseFilename, geoIpDatabaseFilename)).thenReturn(geoIpDatabase)

  val jsonSerializer = new JsonSerializer
  val options = jsonSerializer.serialize(
    Map(OptionsLiterals.ipv4DatField -> geoIpDatabaseFilename))

  val schema = createSchema

  val srcIpStream = new TStreamStreamDomain(StreamNames.srcIpStream, mock[TStreamServiceDomain], 1)
  val srcDstStream = new TStreamStreamDomain(StreamNames.srcDstStream, mock[TStreamServiceDomain], 1)

  val inputStream = new TStreamStreamDomain("input-stream", mock[TStreamServiceDomain], 1)
  val batchInstance = mock[BatchInstanceDomain]
  when(batchInstance.getInputsWithoutStreamMode).thenReturn(Array(inputStream.name))


  val data = Seq(
    Seq(
      createSflowRecord("80.80.80.80", "85.85.85.85", 80, 804),
      createSflowRecord("20.20.20.20", "25.25.25.25", 20, 201),
      createSflowRecord("10.10.10.10", "15.15.15.15", 10, 103)),
    Seq(
      createSflowRecord("20.20.20.20", "25.25.25.25", 20, 203),
      createSflowRecord("30.30.30.30", "35.35.35.35", 30, 301),
      createSflowRecord("20.20.20.20", "25.25.25.25", 20, 204)),
    Seq(
      createSflowRecord("60.60.60.60", "65.65.65.65", 60, 602),
      createSflowRecord("50.50.50.50", "55.55.55.55", 50, 505),
      createSflowRecord("30.30.30.30", "35.35.35.35", 30, 302)),
    Seq(
      createSflowRecord("70.70.70.70", "75.75.75.75", 70, 700),
      createSflowRecord("60.60.60.60", "65.65.65.65", 60, 604),
      createSflowRecord("10.10.10.10", "15.15.15.15", 10, 100)),
    Seq(
      createSflowRecord("10.10.10.10", "15.15.15.15", 10, 105),
      createSflowRecord("80.80.80.80", "85.85.85.85", 80, 802),
      createSflowRecord("60.60.60.60", "65.65.65.65", 60, 600)),
    Seq(
      createSflowRecord("40.40.40.40", "45.45.45.45", 40, 400),
      createSflowRecord("20.20.20.20", "25.25.25.25", 20, 202),
      createSflowRecord("40.40.40.40", "45.45.45.45", 40, 402)),
    Seq(
      createSflowRecord("50.50.50.50", "55.55.55.55", 50, 503),
      createSflowRecord("70.70.70.70", "75.75.75.75", 70, 704),
      createSflowRecord("80.80.80.80", "85.85.85.85", 80, 803)),
    Seq(
      createSflowRecord("40.40.40.40", "45.45.45.45", 40, 403),
      createSflowRecord("80.80.80.80", "85.85.85.85", 80, 805)),
    Seq(
      createSflowRecord("30.30.30.30", "35.35.35.35", 30, 305),
      createSflowRecord("80.80.80.80", "85.85.85.85", 80, 801),
      createSflowRecord("30.30.30.30", "35.35.35.35", 30, 304)),
    Seq(
      createSflowRecord("10.10.10.10", "15.15.15.15", 10, 101),
      createSflowRecord("50.50.50.50", "55.55.55.55", 50, 504),
      createSflowRecord("50.50.50.50", "55.55.55.55", 50, 500)),
    Seq(
      createSflowRecord("40.40.40.40", "45.45.45.45", 40, 405),
      createSflowRecord("50.50.50.50", "55.55.55.55", 50, 501),
      createSflowRecord("50.50.50.50", "55.55.55.55", 50, 502)),
    Seq(
      createSflowRecord("60.60.60.60", "65.65.65.65", 60, 603),
      createSflowRecord("70.70.70.70", "75.75.75.75", 70, 702),
      createSflowRecord("40.40.40.40", "45.45.45.45", 40, 401)),
    Seq(
      createSflowRecord("60.60.60.60", "65.65.65.65", 60, 605),
      createSflowRecord("70.70.70.70", "75.75.75.75", 70, 703),
      createSflowRecord("60.60.60.60", "65.65.65.65", 60, 601)),
    Seq(
      createSflowRecord("10.10.10.10", "15.15.15.15", 10, 104),
      createSflowRecord("20.20.20.20", "25.25.25.25", 20, 200),
      createSflowRecord("70.70.70.70", "75.75.75.75", 70, 705)),
    Seq(
      createSflowRecord("70.70.70.70", "75.75.75.75", 70, 701),
      createSflowRecord("40.40.40.40", "45.45.45.45", 40, 404),
      createSflowRecord("10.10.10.10", "15.15.15.15", 10, 102)),
    Seq(
      createSflowRecord("30.30.30.30", "35.35.35.35", 30, 300),
      createSflowRecord("80.80.80.80", "85.85.85.85", 80, 800),
      createSflowRecord("30.30.30.30", "35.35.35.35", 30, 303),
      createSflowRecord("20.20.20.20", "25.25.25.25", 20, 205)))


  "Executor" should "properly process the correct records" in new Simulator {
    val window = 6
    val slidingInterval = 3

    simulator.prepareState(initialState)
    val transactions = data.map(sflowRecords =>
      (simulator.prepareTstream(sflowRecords.map(sflowToAvro), inputStream.name), sflowRecords))

    val results = simulator.process(window = window, slidingInterval = slidingInterval)
    val expected = buildExpectedResult(transactions, window, slidingInterval)

    sortResult(results.simulationResult) shouldBe sortResult(expected.simulationResult)
    results.remainingEnvelopes shouldBe expected.remainingEnvelopes
  }

  it should "not fail with the incorrect records" in new Simulator {
    val wrongSchema = SchemaBuilder.record("wrongRecord").fields()
      .name(FieldsNames.dstIP).`type`().intType().noDefault().endRecord()

    def wrongRecord(dstIP: Int): Record = {
      val record = new Record(wrongSchema)
      record.put(FieldsNames.dstIP, dstIP)

      record
    }

    val wrongData = Seq(
      Seq(
        wrongRecord(1),
        wrongRecord(2)),
      Seq(
        wrongRecord(3),
        wrongRecord(4)),
      Seq(
        wrongRecord(5),
        wrongRecord(6)),
      Seq(
        wrongRecord(7),
        wrongRecord(8)),
      Seq(
        wrongRecord(9),
        wrongRecord(10)),
      Seq(
        wrongRecord(11),
        wrongRecord(12)),
      Seq(
        wrongRecord(13),
        wrongRecord(14)))

    val window = 4
    val slidingInterval = 2
    simulator.prepareState(initialState)

    val transactions = wrongData.map(records =>
      (simulator.prepareTstream(records, inputStream.name), records))

    val results = simulator.process(window = window, slidingInterval = slidingInterval)
    val expectedRemainingEnvelopes = transactions.takeRight(
      countRemainingEnvelopes(transactions.length, window, slidingInterval)).map(buildEnvelope)
    val expectedResults = BatchSimulationResult(SimulationResult(Seq.empty, initialState), expectedRemainingEnvelopes)

    results shouldBe expectedResults
  }


  trait Simulator {
    val manager = new ModuleEnvironmentManagerMock(stateStorage, options, Array(srcIpStream, srcDstStream), fileStorage)
    val executor = new Executor(manager)
    val batchCollector = new SflowBatchCollector(batchInstance, mock[BatchStreamingPerformanceMetrics], Array(inputStream))
    val simulator = new BatchEngineSimulator(executor, manager, batchCollector)
  }


  def sortResult(result: SimulationResult): SimulationResult = {
    SimulationResult(
      result.streamDataList.map { streamData =>

        StreamData(
          streamData.stream,
          streamData.partitionDataList.map { partitionData =>
            PartitionData(partitionData.partition, partitionData.dataList.sortBy(_.hashCode()))
          })
      }.sortBy(_.stream),
      result.state)
  }

  def buildExpectedResult(transactions: Seq[(Long, Seq[SflowRecord])],
                          window: Int,
                          slidingInterval: Int): BatchSimulationResult = {
    val srcIpList: mutable.Buffer[SrcIp] = mutable.Buffer.empty[SrcIp]
    val srcDstList: mutable.Buffer[SrcDstAs] = mutable.Buffer.empty[SrcDstAs]

    @tailrec
    def inner(transactions: Seq[(Long, Seq[SflowRecord])]): Seq[(Long, Seq[SflowRecord])] = {
      if (transactions.length >= slidingInterval) {
        val tuple = buildOutputEntities(transactions.take(slidingInterval).map(_._2))
        srcIpList ++= tuple._1
        srcDstList ++= tuple._2

        inner(transactions.drop(slidingInterval))
      } else transactions
    }

    val remainingEnvelopes = {
      if (transactions.length >= window) {
        val a = buildOutputEntities(transactions.take(window).map(_._2))
        srcIpList ++= a._1
        srcDstList ++= a._2
        inner(transactions.drop(window))
      } else transactions
    }.map(x => (x._1, x._2.map(sflowToAvro))).map(buildEnvelope)

    BatchSimulationResult(
      SimulationResult(
        Seq(
          StreamData(StreamNames.srcDstStream, Seq(PartitionData(0, srcDstList))),
          StreamData(StreamNames.srcIpStream, Seq(PartitionData(0, srcIpList)))),
        initialState),
      remainingEnvelopes)
  }

  def buildOutputEntities(sflowRecordsList: Seq[Seq[SflowRecord]]): (mutable.Iterable[SrcIp], mutable.Iterable[SrcDstAs]) = {
    val srcIpMap = mutable.Map.empty[String, Int]
    val srcDstMap = mutable.Map.empty[(Int, Int), Int]
    sflowRecordsList.flatten.foreach {
      case SflowRecord(_, _, _, _, _, _, _, _, _, _, srcIP, _, _, _, _, _, _, _, packetSize, _, samplingRate, srcAs, dstAs) =>
        val traffic = packetSize * samplingRate
        srcIpMap(srcIP) = srcIpMap.getOrElse(srcIP, 0) + traffic
        srcDstMap((srcAs, dstAs)) = srcDstMap.getOrElse((srcAs, dstAs), 0) + traffic
    }

    (srcIpMap.map(SrcIp(_)), srcDstMap.map(SrcDstAs(_)))
  }

  def buildEnvelope(transaction: (Long, Seq[Record])): TStreamEnvelope[Record] = {
    transaction match {
      case (id, records) =>
        val envelope = new TStreamEnvelope(
          mutable.Queue(records: _*),
          SimulatorConstants.defaultConsumerName)
        envelope.id = id
        envelope.stream = inputStream.name

        envelope
    }
  }

  def countRemainingEnvelopes(envelopes: Int, window: Int, slidingInterval: Int): Int = {
    if (envelopes >= window)
      (envelopes - window) % slidingInterval
    else
      envelopes
  }


  def createSchema: Schema = {
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

  def fillGeoIpAs(sflowRecord: SflowRecord): SflowRecord = {
    sflowRecord.srcAs = Try(geoIp.resolveAs(sflowRecord.srcIP)).getOrElse(0)
    sflowRecord.dstAs = Try(geoIp.resolveAs(sflowRecord.dstIP)).getOrElse(0)

    sflowRecord
  }

  def createSflowRecord(srcIP: String, dstIP: String, packetSize: Int, samplingRate: Int): SflowRecord = {
    val sflowRecord = SflowRecord(
      srcIP = srcIP,
      dstIP = dstIP,
      packetSize = packetSize,
      samplingRate = samplingRate,
      timestamp = 1499650706,
      name = "sflow-record",
      agentAddress = "10.10.10.10",
      inputPort = 1234,
      outputPort = 4321,
      srcMAC = "11:22:33:44:55:66",
      dstMAC = "66:55:44:33:22:11",
      ethernetType = "type1",
      inVlan = 10,
      outVlan = 11,
      ipProtocol = 4,
      ipTos = "tos",
      ipTtl = 64,
      udpSrcPort = 1000,
      udpDstPort = 1001,
      tcpFlags = "tcp-flags",
      ipSize = 32)

    fillGeoIpAs(sflowRecord)
  }

  def sflowToAvro(sflowRecord: SflowRecord): Record = {
    val avroRecord = new Record(schema)
    avroRecord.put(FieldsNames.timestamp, new Utf8(sflowRecord.timestamp.toString))
    avroRecord.put(FieldsNames.name, new Utf8(sflowRecord.name.toString))
    avroRecord.put(FieldsNames.agentAddress, new Utf8(sflowRecord.agentAddress.toString))
    avroRecord.put(FieldsNames.inputPort, new Utf8(sflowRecord.inputPort.toString))
    avroRecord.put(FieldsNames.outputPort, new Utf8(sflowRecord.outputPort.toString))
    avroRecord.put(FieldsNames.srcMAC, new Utf8(sflowRecord.srcMAC.toString))
    avroRecord.put(FieldsNames.dstMAC, new Utf8(sflowRecord.dstMAC.toString))
    avroRecord.put(FieldsNames.ethernetType, new Utf8(sflowRecord.ethernetType.toString))
    avroRecord.put(FieldsNames.inVlan, new Utf8(sflowRecord.inVlan.toString))
    avroRecord.put(FieldsNames.outVlan, new Utf8(sflowRecord.outVlan.toString))
    avroRecord.put(FieldsNames.srcIP, new Utf8(sflowRecord.srcIP.toString))
    avroRecord.put(FieldsNames.dstIP, new Utf8(sflowRecord.dstIP.toString))
    avroRecord.put(FieldsNames.ipProtocol, new Utf8(sflowRecord.ipProtocol.toString))
    avroRecord.put(FieldsNames.ipTos, new Utf8(sflowRecord.ipTos.toString))
    avroRecord.put(FieldsNames.ipTtl, new Utf8(sflowRecord.ipTtl.toString))
    avroRecord.put(FieldsNames.udpSrcPort, new Utf8(sflowRecord.udpSrcPort.toString))
    avroRecord.put(FieldsNames.udpDstPort, new Utf8(sflowRecord.udpDstPort.toString))
    avroRecord.put(FieldsNames.tcpFlags, new Utf8(sflowRecord.tcpFlags.toString))
    avroRecord.put(FieldsNames.packetSize, new Utf8(sflowRecord.packetSize.toString))
    avroRecord.put(FieldsNames.ipSize, new Utf8(sflowRecord.ipSize.toString))
    avroRecord.put(FieldsNames.samplingRate, new Utf8(sflowRecord.samplingRate.toString))

    avroRecord
  }
}
