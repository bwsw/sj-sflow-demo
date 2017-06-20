package com.bwsw.sj.examples.sflow.module.output.fallback

import com.bwsw.sj.engine.core.environment.OutputEnvironmentManager
import com.bwsw.sj.engine.core.simulation.{JdbcRequestBuilder, OutputEngineSimulator}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.util.Utf8
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
  val insertionQueryRegexPrefix = s"INSERT INTO $table \\(id,line,$transactionField\\) VALUES \\('[-0-9a-f]*',"
  val recordField = "data"
  val schema = SchemaBuilder.record("fallback").fields()
    .name(recordField).`type`().stringType().noDefault()
    .endRecord()

  "Executor" should "work properly before first checkpoint" in new TestPreparation {
    val transactions = Seq(
      Seq(
        "incorrect input 1",
        "incorrect input 2"),
      Seq(
        "incorrect input 3"),
      Seq(
        "incorrect input 4"))

    val expectedQueriesData = transactions.flatMap { transaction =>
      val transactionId = engineSimulator.prepare(transaction.map(createRecord))
      transactionId +: transaction.map(line => (transactionId, line))
    }

    val queries = engineSimulator.process()
    queries.length shouldBe expectedQueriesData.length

    expectedQueriesData.zip(queries).foreach {
      case (transactionId: Long, statement) =>
        val expectedQuery = deletionQueryPrefix + transactionId
        statement.getQuery shouldBe expectedQuery

      case ((transactionId: Long, line: String), statement) =>
        val expectedQueryRegex = createInsertionRegex(transactionId, line)
        statement.getQuery should include regex expectedQueryRegex

      case _ => throw new IllegalStateException
    }
  }

  it should "work properly after first checkpoint" in new TestPreparation {
    // "perform" first checkpoint
    engineSimulator.wasFirstCheckpoint = true

    val transactions = Seq(
      Seq(
        "incorrect input 5",
        "incorrect input 6"),
      Seq(
        "incorrect input 7"),
      Seq(
        "incorrect input 8",
        "incorrect input 9"))

    val expectedQueriesData = transactions.flatMap { transaction =>
      val transactionId = engineSimulator.prepare(transaction.map(createRecord))
      transaction.map(line => (transactionId, line))
    }

    val queries = engineSimulator.process()
    queries.length shouldBe expectedQueriesData.length

    expectedQueriesData.zip(queries).foreach {
      case ((transactionId, line), statement) =>
        val expectedQueryRegex = createInsertionRegex(transactionId, line)
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

  def createInsertionRegex(transactionId: Long, line: String): String =
    insertionQueryRegexPrefix + s"'$line',$transactionId\\)"

  def createRecord(line: String): Record = {
    val record = new Record(schema)
    record.put(recordField, new Utf8(line))
    record
  }
}
