package com.bwsw.sj.examples.sflow.module.output.srcdst

import com.bwsw.sj.engine.core.environment.OutputEnvironmentManager
import com.bwsw.sj.engine.core.simulation.{JdbcRequestBuilder, OutputEngineSimulator}
import com.bwsw.sj.examples.sflow.common.JdbcFieldsNames.{dstAsField, idField, srcAsField, trafficField}
import com.bwsw.sj.examples.sflow.common.SrcDstAs
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
    s"\\($idField,$srcAsField,$dstAsField,$trafficField,$transactionField\\) VALUES \\('[-0-9a-f]*',"

  "Executor" should "work properly before first checkpoint" in new TestPreparation {
    val transactions = Map(
      0l -> Seq(
        SrcDstAs(10, 100, 1000),
        SrcDstAs(20, 200, 2000)),
      1l -> Seq(
        SrcDstAs(30, 300, 3000)),
      2l -> Seq(
        SrcDstAs(40, 400, 4000)))

    transactions.foreach {
      case (_, transaction) => engineSimulator.prepare(transaction)
    }

    val queries = engineSimulator.process()

    val expectedQueriesData = transactions.toSeq.flatMap {
      case (transactionId, transaction) =>
        transactionId +: transaction.map(srcDstAs => (transactionId, srcDstAs))
    }
    queries.length shouldBe expectedQueriesData.length

    expectedQueriesData.zip(queries).foreach {
      case (transactionId: Long, statement) =>
        val expectedQuery = deletionQueryPrefix + transactionId
        statement.getQuery shouldBe expectedQuery

      case ((transactionId: Long, srcDstAs: SrcDstAs), statement) =>
        val expectedQueryRegex = createInsertionRegex(transactionId, srcDstAs)
        statement.getQuery should include regex expectedQueryRegex

      case _ => throw new IllegalStateException
    }
  }

  it should "work properly after first checkpoint" in new TestPreparation {
    // "perform" first checkpoint
    engineSimulator.wasFirstCheckpoint = true

    val transactions = Map(
      0l -> Seq(
        SrcDstAs(50, 500, 5000),
        SrcDstAs(60, 600, 6000)),
      1l -> Seq(
        SrcDstAs(70, 700, 7000)),
      2l -> Seq(
        SrcDstAs(80, 800, 8000),
        SrcDstAs(90, 900, 9000)))

    transactions.foreach {
      case (_, transaction) => engineSimulator.prepare(transaction)
    }

    val queries = engineSimulator.process()

    val expectedQueriesData = transactions.toSeq.flatMap {
      case (transactionId, transaction) =>
        transaction.map(srcDstAs => (transactionId, srcDstAs))
    }
    queries.length shouldBe expectedQueriesData.length

    expectedQueriesData.zip(queries).foreach {
      case ((transactionId, srcDstAs), statement) =>
        val expectedQueryRegex = createInsertionRegex(transactionId, srcDstAs)
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

  def createInsertionRegex(transactionId: Long, srcDstAs: SrcDstAs): String =
    insertionQueryRegexPrefix + s"${srcDstAs.srcAs},${srcDstAs.dstAs},${srcDstAs.traffic},$transactionId\\)"
}
