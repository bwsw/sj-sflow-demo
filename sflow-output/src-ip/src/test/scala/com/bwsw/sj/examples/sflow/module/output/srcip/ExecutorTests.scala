package com.bwsw.sj.examples.sflow.module.output.srcip

import com.bwsw.sj.engine.core.environment.OutputEnvironmentManager
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
  val deletionQueryPrefix = s"DELETE FROM $table WHERE $transactionField = "
  val insertionQueryRegexPrefix = s"INSERT INTO $table " +
    s"\\($idField,$srcIpField,$trafficField,$transactionField\\) VALUES \\('[-0-9a-f]*',"

  "Executor" should "work properly before first checkpoint" in new TestPreparation {
    val transactions = Map(
      0l -> Seq(
        SrcIp("11.11.11.11", 1000),
        SrcIp("22.22.22.22", 2000)),
      1l -> Seq(
        SrcIp("33.33.33.33", 3000)),
      2l -> Seq(
        SrcIp("44.44.44.44", 4000)))


    transactions.foreach {
      case (_, transaction) => engineSimulator.prepare(transaction)
    }

    val queries = engineSimulator.process()

    val expectedQueriesData = transactions.toSeq.flatMap {
      case (transactionId, transaction) =>
        transactionId +: transaction.map(srcIp => (transactionId, srcIp))
    }
    queries.length shouldBe expectedQueriesData.length

    expectedQueriesData.zip(queries).foreach {
      case (transactionId: Long, statement) =>
        val expectedQuery = deletionQueryPrefix + transactionId
        statement.getQuery shouldBe expectedQuery

      case ((transactionId: Long, srcIp: SrcIp), statement) =>
        val expectedQueryRegex = createInsertionRegex(transactionId, srcIp)
        statement.getQuery should fullyMatch regex expectedQueryRegex

      case _ => throw new IllegalStateException
    }
  }

  it should "work properly after first checkpoint" in new TestPreparation {
    // "perform" first checkpoint
    engineSimulator.wasFirstCheckpoint = true

    val transactions = Map(
      0l -> Seq(
        SrcIp("55.55.55.55", 5000),
        SrcIp("66.66.66.66", 6000)),
      1l -> Seq(
        SrcIp("77.77.77.77", 7000)),
      2l -> Seq(
        SrcIp("88.88.88.88", 8000),
        SrcIp("99.99.99.99", 9000)))

    transactions.foreach {
      case (_, transaction) => engineSimulator.prepare(transaction)
    }

    val queries = engineSimulator.process()

    val expectedQueriesData = transactions.toSeq.flatMap {
      case (transactionId, transaction) =>
        transaction.map(srcIp => (transactionId, srcIp))
    }
    queries.length shouldBe expectedQueriesData.length

    expectedQueriesData.zip(queries).foreach {
      case ((transactionId, srcIp), statement) =>
        val expectedQueryRegex = createInsertionRegex(transactionId, srcIp)
        statement.getQuery should include regex expectedQueryRegex
    }
  }

  trait TestPreparation {
    val manager = mock[OutputEnvironmentManager]
    when(manager.isCheckpointInitiated).thenReturn(false)

    val executor = new Executor(manager)
    val requestBuilder = new JdbcRequestBuilder(executor.getOutputEntity, table)
    val engineSimulator = new OutputEngineSimulator(executor, requestBuilder, manager)
  }

  def createInsertionRegex(transactionId: Long, srcIp: SrcIp): String =
    insertionQueryRegexPrefix + s"'${srcIp.srcIP}',${srcIp.traffic},$transactionId\\)"
}
