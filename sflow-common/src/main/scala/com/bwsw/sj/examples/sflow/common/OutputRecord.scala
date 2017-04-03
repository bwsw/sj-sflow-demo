package com.bwsw.sj.examples.sflow.common

/**
  * @author Pavel Tomskikh
  */
case class OutputRecord(srcIp: String, srcAs: Int, dstIp: String, dstAs: Int, traffic: Int) extends Serializable
