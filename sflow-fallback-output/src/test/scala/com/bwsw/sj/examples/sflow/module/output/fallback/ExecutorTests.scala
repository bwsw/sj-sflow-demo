package com.bwsw.sj.examples.sflow.module.output.fallback

import com.bwsw.sj.common.dal.model.service.TStreamServiceDomain
import com.bwsw.sj.common.dal.model.stream.TStreamStreamDomain
import com.bwsw.sj.engine.core.environment.OutputEnvironmentManager
import com.bwsw.sj.engine.core.output.types.jdbc.JdbcCommandBuilder
import com.bwsw.sj.engine.core.simulation.mock.jdbc.JdbcClientMock
import com.bwsw.sj.engine.core.simulation.{JdbcRequestBuilder, OutputEngineSimulator}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.util.Utf8
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

  val recordField = "data"
  val schema = SchemaBuilder.record("fallback").fields()
    .name(recordField).`type`().stringType().noDefault()
    .endRecord()

  val options = "{}"
  val outputStream = new TStreamStreamDomain("output-stream", mock[TStreamServiceDomain], 1)
  val manager: OutputEnvironmentManager = new OutputEnvironmentManager(options, Array(outputStream))
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
        "incorrect input 1",
        "incorrect input 2"),
      Seq(
        "incorrect input 3"),
      Seq(
        "incorrect input 4"))

    val expectedPreparedStatements = transactions.flatMap { transaction =>
      val transactionId = engineSimulator.prepare(transaction.map(createRecord))

      val deleteStatement = commandBuilder.buildDelete(transactionId)
      val insertStatements = transaction.map { line =>
        commandBuilder.buildInsert(transactionId, createFieldsMap(line))
      }

      deleteStatement +: insertStatements
    }

    val preparedStatements = engineSimulator.process()
    preparedStatements.foreach { preparedStatement =>
      if (!preparedStatement.getQuery.startsWith("DELETE")) {
        // replace value of "id" field because it has random value and we can't validate prepared statement
        // see com.bwsw.sj.examples.sflow.module.output.fallback.data.Fallback
        preparedStatement.setString(idFieldIndex, dataId)
      }
    }

    preparedStatements shouldBe expectedPreparedStatements
  }

  it should "work properly after first checkpoint" in {
    val engineSimulator = new OutputEngineSimulator(executor, requestBuilder, manager)
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

    val expectedPreparedStatements = transactions.flatMap { transaction =>
      val transactionId = engineSimulator.prepare(transaction.map(createRecord))

      transaction.map { line =>
        commandBuilder.buildInsert(transactionId, createFieldsMap(line))
      }
    }

    val preparedStatements = engineSimulator.process()
    preparedStatements.foreach { preparedStatement =>
      // replace value of "id" field because it has random value and we can't validate prepared statement
      // see com.bwsw.sj.examples.sflow.module.output.fallback.data.Fallback
      preparedStatement.setString(idFieldIndex, dataId)
    }

    preparedStatements shouldBe expectedPreparedStatements
  }

  def createRecord(line: String): Record = {
    val record = new Record(schema)
    record.put(recordField, new Utf8(line))
    record
  }

  def createFieldsMap(line: String): Map[String, String] = {
    Map(
      "id" -> dataId,
      "line" -> line)
  }
}
