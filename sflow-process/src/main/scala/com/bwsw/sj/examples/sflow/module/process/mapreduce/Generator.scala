package com.bwsw.sj.examples.sflow.module.process.mapreduce

import java.util
import java.util.concurrent.TimeUnit

import com.bwsw.sj.examples.sflow.common.SflowRecord
import com.bwsw.sj.examples.sflow.module.process.mapreduce.mappers._
import com.hazelcast.config._
import com.hazelcast.core.{Hazelcast, HazelcastInstance, IMap}
import com.hazelcast.mapreduce.{JobCompletableFuture, JobTracker, KeyValueSource}

import scala.collection.JavaConverters._
import scala.util.Try

class Generator {
  var hazelcastMapName = "hazelcast"
  var trackerName = "tracker"
  var hazelcastInstance: Option[HazelcastInstance] = None

  var tracker: Option[JobTracker] = None
  var source: Option[KeyValueSource[String, SflowRecord]] = None


  def SrcAsReduceResult(): collection.mutable.Map[Int, Int] = {
    val tracker = getTracker
    val source = getSource
    val job = tracker.newJob(source)
    val future = job.mapper(new SrcAsMapper()).reducer[Int](new CommonReducerFactory[Int]()).submit()

    getResult(future)
  }

  def DstAsReduceResult(): collection.mutable.Map[Int, Int] = {
    val tracker = getTracker
    val source = getSource
    val job = tracker.newJob(source)
    val future = job.mapper(new DstAsMapper()).reducer[Int](new CommonReducerFactory[Int]).submit()

    getResult(future)
  }

  def SrcDstReduceResult(): collection.mutable.Map[Int Tuple2 Int, Int] = {
    val tracker = getTracker
    val source = getSource
    val job = tracker.newJob(source)
    val future = job.mapper(new SrcDstAsMapper()).reducer[Int](new CommonReducerFactory[Int Tuple2 Int]).submit()

    getResult(future)
  }

  def SrcIpReduceResult(): collection.mutable.Map[String, Int] = {
    val tracker = getTracker
    val source = getSource
    val job = tracker.newJob(source)
    val future = job.mapper(new SrcIpMapper()).reducer[Int](new CommonReducerFactory[String]).submit()

    getResult(future)
  }

  def DstIpReduceResult(): collection.mutable.Map[String, Int] = {
    val tracker = getTracker
    val source = getSource
    val job = tracker.newJob(source)
    val future = job.mapper(new DstIpMapper()).reducer[Int](new CommonReducerFactory[String]).submit()

    getResult(future)
  }

  def getSource: KeyValueSource[String, SflowRecord] = {
    if (source.isEmpty) source = Some(KeyValueSource.fromMap[String, SflowRecord](getMap))
    source.get
  }

  def getTracker: JobTracker = {
    if (tracker.isEmpty) tracker = Some(getHazelcastInstance.getJobTracker(trackerName))
    tracker.get
  }

  def putRecords(records: Iterable[SflowRecord]) = {
    val imap: IMap[String, SflowRecord] = getHazelcastInstance.getMap[String, SflowRecord](hazelcastMapName)
    records.foreach(sflowRecord => imap.put(uuid, sflowRecord))
  }

  def getMap: IMap[String, SflowRecord] =
    getHazelcastInstance.getMap[String, SflowRecord](hazelcastMapName)

  def setHazelcastMapName(mapName: String) =
    hazelcastMapName = mapName


  def getHazelcastInstance: HazelcastInstance = {
    if (hazelcastInstance.isEmpty) hazelcastInstance = Some(Hazelcast.newHazelcastInstance(getHazelcastConfig))
    hazelcastInstance.get
  }

  def getHazelcastConfig: Config = {
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

  def uuid = java.util.UUID.randomUUID.toString

  def clear() = getMap.clear()


  private def getResult[K, V](future: JobCompletableFuture[util.Map[K, V]]): collection.mutable.Map[K, V] =
    Try(future.get(10, TimeUnit.SECONDS).asScala).getOrElse(collection.mutable.Map.empty)
}
