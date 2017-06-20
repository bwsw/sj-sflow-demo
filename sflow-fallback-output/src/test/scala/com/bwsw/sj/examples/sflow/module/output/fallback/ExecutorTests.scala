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

  "Executor" should "work properly" in {
    val manager = mock[OutputEnvironmentManager]
    when(manager.isCheckpointInitiated).thenReturn(false)

    val executor = new Executor(manager)
    val requestBuilder = new JdbcRequestBuilder(executor.getOutputEntity, table)
    val engineSimulator = new OutputEngineSimulator(executor, requestBuilder, manager)

    val transactionsBeforeCheckpoint = Map(
      0l -> Seq(
        "incorrect input 1",
        "incorrect input 2"),
      1l -> Seq(
        "incorrect input 3"),
      2l -> Seq(
        "incorrect input 4"))

    val transactionsAfterCheckpoint = Map(
      3l -> Seq(
        "incorrect input 5",
        "incorrect input 6"),
      4l -> Seq(
        "incorrect input 7"),
      5l -> Seq(
        "incorrect input 8",
        "incorrect input 9"))

    transactionsBeforeCheckpoint.foreach {
      case (_, transaction) =>
        engineSimulator.prepare(transaction.map(createRecord))
    }

    val queriesBeforeCheckpoint = engineSimulator.process()

    val expectedQueriesBeforeCheckpoint = transactionsBeforeCheckpoint.toSeq.flatMap {
      case (transactionId, content) =>
        transactionId +: content.map(line => (transactionId, line))
    }
    queriesBeforeCheckpoint.length shouldBe expectedQueriesBeforeCheckpoint.size

    expectedQueriesBeforeCheckpoint.zip(queriesBeforeCheckpoint).foreach {
      case (transactionId: Long, statement) =>
        val expectedQuery = deletionQueryPrefix + transactionId
        statement.getQuery shouldBe expectedQuery

      case ((transactionId: Long, line: String), statement) =>
        val expectedQueryRegex = createInsertionRegex(transactionId, line)
        statement.getQuery should include regex expectedQueryRegex

      case _ => throw new IllegalStateException
    }


    // "perform" first checkpoint
    engineSimulator.wasFirstCheckpoint = true

    transactionsAfterCheckpoint.foreach {
      case (_, transaction) => engineSimulator.prepare(transaction.map(createRecord))
    }

    val queriesAfterCheckpoint = engineSimulator.process()

    val expectedQueriesAfterCheckpoint = transactionsAfterCheckpoint.toSeq.flatMap {
      case (transactionId, transaction) =>
        transaction.map(line => (transactionId, line))
    }
    queriesAfterCheckpoint.length shouldBe expectedQueriesAfterCheckpoint.size

    expectedQueriesAfterCheckpoint.zip(queriesAfterCheckpoint).foreach {
      case ((transactionId, line), statement) =>
        val expectedQueryRegex = createInsertionRegex(transactionId, line)
        statement.getQuery should include regex expectedQueryRegex
    }
  }

  def createInsertionRegex(transactionId: Long, line: String): String =
    insertionQueryRegexPrefix + s"'$line',$transactionId\\)"

  def createRecord(line: String): Record = {
    val record = new Record(schema)
    record.put(recordField, new Utf8(line))
    record
  }
}
