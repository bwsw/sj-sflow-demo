package com.bwsw.sj.examples.sflow.module.output.srcdst

import com.bwsw.sj.common.dal.model.service.TStreamServiceDomain
import com.bwsw.sj.common.dal.model.stream.TStreamStreamDomain
import com.bwsw.sj.common.engine.core.environment.OutputEnvironmentManager
import com.bwsw.sj.engine.core.output.types.jdbc.JdbcCommandBuilder
import com.bwsw.sj.engine.core.simulation.output.mock.jdbc.JdbcClientMock
import com.bwsw.sj.engine.core.simulation.output.{JdbcRequestBuilder, OutputEngineSimulator}
import com.bwsw.sj.examples.sflow.common.JdbcFieldsNames.{dstAsField, srcAsField, trafficField}
import com.bwsw.sj.examples.sflow.common.SrcDstAs
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
  * Tests for [[Executor]]
  *
  * @author Pavel Tomskikh
  */
class ExecutorTests extends FlatSpec with Matchers with MockitoSugar {

  val transactionField = "txn"
  val table = "output"

  val options = "{}"
  val outputStream = new TStreamStreamDomain("output-stream", mock[TStreamServiceDomain], 1)
  val manager: OutputEnvironmentManager = new OutputEnvironmentManager(options, Array(outputStream))
  val executor = new Executor(manager)
  val requestBuilder = new JdbcRequestBuilder(executor.getOutputEntity, table)

  val jdbcClient = new JdbcClientMock(table)
  val commandBuilder = new JdbcCommandBuilder(jdbcClient, transactionField, executor.getOutputEntity)

  "Executor" should "work properly before first checkpoint" in {
    val engineSimulator = new OutputEngineSimulator(executor, requestBuilder, manager)

    val transactions = Seq(
      Seq(
        SrcDstAs(10, 100, 1000),
        SrcDstAs(20, 200, 2000)),
      Seq(
        SrcDstAs(30, 300, 3000)),
      Seq(
        SrcDstAs(40, 400, 4000)))

    val expectedPreparedStatements = transactions.flatMap { transaction =>
      val transactionId = engineSimulator.prepare(transaction)

      val deleteStatement = commandBuilder.buildDelete(transactionId)
      val insertStatements = transaction.map { srcDstAs =>
        commandBuilder.buildInsert(transactionId, createFieldsMap(srcDstAs))
      }

      deleteStatement +: insertStatements
    }

    val preparedStatements = engineSimulator.process()
    preparedStatements shouldBe expectedPreparedStatements
  }

  it should "work properly after first checkpoint" in {
    val engineSimulator = new OutputEngineSimulator(executor, requestBuilder, manager)
    // "perform" first checkpoint
    engineSimulator.beforeFirstCheckpoint = false

    val transactions = Seq(
      Seq(
        SrcDstAs(50, 500, 5000),
        SrcDstAs(60, 600, 6000)),
      Seq(
        SrcDstAs(70, 700, 7000)),
      Seq(
        SrcDstAs(80, 800, 8000),
        SrcDstAs(90, 900, 9000)))

    val expectedPreparedStatements = transactions.flatMap { transaction =>
      val transactionId = engineSimulator.prepare(transaction)

      transaction.map { srcDstAs =>
        commandBuilder.buildInsert(transactionId, createFieldsMap(srcDstAs))
      }
    }

    val preparedStatements = engineSimulator.process()
    preparedStatements shouldBe expectedPreparedStatements
  }

  def createFieldsMap(srcDstAs: SrcDstAs): Map[String, Any] = {
    Map(
      srcAsField -> srcDstAs.srcAs,
      dstAsField -> srcDstAs.dstAs,
      trafficField -> srcDstAs.traffic)
  }
}
