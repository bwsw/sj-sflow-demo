package com.bwsw.sj.examples.sflow.module.output.srcip

import com.bwsw.sj.engine.core.environment.OutputEnvironmentManager
import com.bwsw.sj.engine.core.output.types.jdbc.JdbcCommandBuilder
import com.bwsw.sj.engine.core.simulation.mock.jdbc.{JdbcClientMock, PreparedStatementMock}
import com.bwsw.sj.engine.core.simulation.{JdbcRequestBuilder, OutputEngineSimulator}
import com.bwsw.sj.examples.sflow.common.JdbcFieldsNames.{idField, srcIpField, trafficField}
import com.bwsw.sj.examples.sflow.common.SrcIp
import org.mockito.Mockito.when
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

  val manager = mock[OutputEnvironmentManager]
  when(manager.isCheckpointInitiated).thenReturn(false)
  val executor = new Executor(manager)
  val requestBuilder = new JdbcRequestBuilder(executor.getOutputEntity, table)

  val jdbcClient = new JdbcClientMock(table)
  val commandBuilder = new JdbcCommandBuilder(jdbcClient, transactionField, executor.getOutputEntity)
  val idFieldIndex = 1
  val dataId = "data id"

  "Executor" should "work properly before first checkpoint" in {
    val engineSimulator = new OutputEngineSimulator(executor, requestBuilder, manager)
    val transactions = Seq(
      Seq(
        SrcIp("11.11.11.11", 1000),
        SrcIp("22.22.22.22", 2000)),
      Seq(
        SrcIp("33.33.33.33", 3000)),
      Seq(
        SrcIp("44.44.44.44", 4000)))

    val expectedPreparedStatements = transactions.flatMap { transaction =>
      val transactionId = engineSimulator.prepare(transaction)

      val deletionStatement = commandBuilder.buildDelete(transactionId)
      val insertionStatements = transaction.map { srcDstAs =>
        commandBuilder.buildInsert(transactionId, createFieldsMap(srcDstAs))
      }

      deletionStatement +: insertionStatements
    }

    val preparedStatements = engineSimulator.process()
    preparedStatements.foreach { preparedStatement =>
      if (!preparedStatement.getQuery.startsWith("DELETE"))
        preparedStatement.setString(idFieldIndex, dataId)
    }

    preparedStatements shouldBe expectedPreparedStatements
  }

  it should "work properly after first checkpoint" in {
    val engineSimulator = new OutputEngineSimulator(executor, requestBuilder, manager)
    // "perform" first checkpoint
    engineSimulator.wasFirstCheckpoint = true

    val transactions = Seq(
      Seq(
        SrcIp("55.55.55.55", 5000),
        SrcIp("66.66.66.66", 6000)),
      Seq(
        SrcIp("77.77.77.77", 7000)),
      Seq(
        SrcIp("88.88.88.88", 8000),
        SrcIp("99.99.99.99", 9000)))

    val expectedPreparedStatements = transactions.flatMap { transaction =>
      val transactionId = engineSimulator.prepare(transaction)

      transaction.map { srcDstAs =>
        commandBuilder.buildInsert(transactionId, createFieldsMap(srcDstAs))
      }
    }

    val preparedStatements = engineSimulator.process()
    preparedStatements.foreach { preparedStatement =>
      preparedStatement.setString(idFieldIndex, dataId)
    }

    preparedStatements shouldBe expectedPreparedStatements
  }

  def createFieldsMap(srcIp: SrcIp): Map[String, Any] = {
    Map(
      idField -> dataId,
      srcIpField -> srcIp.srcIP,
      trafficField -> srcIp.traffic)
  }
}
