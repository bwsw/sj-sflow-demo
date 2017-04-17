package com.bwsw.sj.examples.sflow.common

/**
  * @author Pavel Tomskikh
  */
case class SrcAs(srcAs: Int, traffic: Int) extends Serializable

object SrcAs {
  def apply(tuple: (Int, Int)) = new SrcAs(tuple._1, tuple._2)
}