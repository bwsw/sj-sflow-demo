package com.bwsw.sj.examples.sflow.module.process.mapreduce.mappers

import com.bwsw.sj.examples.sflow.common.SflowRecord
import com.hazelcast.mapreduce.{Context, Mapper}

class SrcAsMapper extends Mapper[String, SflowRecord, Int, Int] {
  override def map(key: String, value: SflowRecord, context: Context[Int, Int]) = {
    context.emit(value.srcAs, value.getTraffic)
  }
}
