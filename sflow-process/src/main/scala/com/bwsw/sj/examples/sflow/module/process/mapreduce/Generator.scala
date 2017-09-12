package com.bwsw.sj.examples.sflow.module.process.mapreduce

import java.util.concurrent.TimeUnit

import com.bwsw.sj.examples.sflow.common.SflowRecord
import com.bwsw.sj.examples.sflow.module.process.mapreduce.mappers._
import com.hazelcast.config._
import com.hazelcast.core.{Hazelcast, HazelcastInstance, IMap}
import com.hazelcast.mapreduce.{JobTracker, KeyValueSource, Mapper}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

class Generator {
  private val hazelcastMapName = "hazelcast"
  private val trackerName = "tracker"

  private lazy val hazelcastInstance: HazelcastInstance = Hazelcast.newHazelcastInstance(getHazelcastConfig)
  private lazy val tracker: JobTracker = hazelcastInstance.getJobTracker(trackerName)
  private lazy val source: KeyValueSource[String, SflowRecord] = KeyValueSource.fromMap[String, SflowRecord](getMap)


  def srcAsReduceResult(): mutable.Map[Int, Int] =
    reduceResult(new SrcAsMapper(), new CommonReducerFactory[Int])

  def dstAsReduceResult(): mutable.Map[Int, Int] =
    reduceResult(new DstAsMapper(), new CommonReducerFactory[Int])

  def srcDstReduceResult(): mutable.Map[(Int, Int), Int] =
    reduceResult(new SrcDstAsMapper(), new CommonReducerFactory[(Int, Int)])

  def srcIpReduceResult(): mutable.Map[String, Int] =
    reduceResult(new SrcIpMapper(), new CommonReducerFactory[String])

  def dstIpReduceResult(): mutable.Map[String, Int] =
    reduceResult(new DstIpMapper(), new CommonReducerFactory[String])

  def putRecords(records: Iterable[SflowRecord]): Unit = {
    val iMap: IMap[String, SflowRecord] = getMap
    records.foreach(sflowRecord => iMap.put(uuid, sflowRecord))
  }

  def clear(): Unit = getMap.clear()


  private def getMap: IMap[String, SflowRecord] =
    hazelcastInstance.getMap[String, SflowRecord](hazelcastMapName)

  private def getHazelcastConfig: Config = {
    val config = new XmlConfigBuilder().build

    val tcpIpConfig = new TcpIpConfig
    val hosts = Array("127.0.0.1").toList.asJava
    tcpIpConfig.setMembers(hosts).setEnabled(true)

    val joinConfig = new JoinConfig
    joinConfig.setMulticastConfig(new MulticastConfig().setEnabled(false))
    joinConfig.setTcpIpConfig(tcpIpConfig)

    val networkConfig = new NetworkConfig
    networkConfig.setJoin(joinConfig)
    val classLoader = getClass.getClassLoader

    config.setNetworkConfig(networkConfig).setClassLoader(classLoader)
  }

  private def uuid: String = java.util.UUID.randomUUID.toString

  private def reduceResult[KeyOut](mapper: Mapper[String, SflowRecord, KeyOut, Int],
                                   reducerFactory: CommonReducerFactory[KeyOut]): mutable.Map[KeyOut, Int] = {
    val job = tracker.newJob(source)
    val future = job.mapper(mapper).reducer[Int](reducerFactory).submit()

    Try(future.get(10, TimeUnit.SECONDS).asScala).getOrElse(mutable.Map.empty)
  }
}
