package com.bwsw.sj.examples.sflow.common

case class SflowRecord(
    timestamp: Long,
    name: String,
    agentAddress: String,
    inputPort: Int,
    outputPort: Int,
    srcMAC: String,
    dstMAC: String,
    ethernetType: String,
    inVlan: Int,
    outVlan: Int,
    srcIP: String,
    dstIP: String,
    ipProtocol: Int,
    ipTos: String,
    ipTtl: Int,
    udpSrcPort: Int,
    udpDstPort: Int,
    tcpFlags: String,
    packetSize: Int,
    ipSize: Int,
    samplingRate: Int,
    var srcAs: Int = 0,
    var dstAs: Int = 0) {

  def getTraffic = packetSize * samplingRate

  def getSrcDstAs = SrcDstAs(srcAs, dstAs, getTraffic)

  def getSrcAs = SrcAs(srcAs, getTraffic)

  def getSrcIp = SrcIp(srcIP, getTraffic)

  def getDstAs = DstAs(dstAs, getTraffic)

  def getDstIp = DstIp(dstIP, getTraffic)
}
