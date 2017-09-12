package com.bwsw.sj.examples.sflow.module.process.mapreduce

import com.hazelcast.mapreduce.Reducer

class CommonReducer extends Reducer[Int, Int] {
  private var sum: Int = 0

  override def reduce(value: Int): Unit =
    sum += value

  override def finalizeReduce(): Int = sum
}
