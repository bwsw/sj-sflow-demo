package com.bwsw.sj.examples.sflow.module.process

import com.bwsw.common.JsonSerializer
import com.bwsw.sj.common.engine.{StreamingValidator, ValidationInfo}

import scala.collection.mutable.ArrayBuffer

class Validator extends StreamingValidator {

  import OptionsLiterals._

  private val serializer = new JsonSerializer(ignoreUnknown = true)
  private val error = s"At least one of fields '$ipv4DatField', '$ipv6DatField' must be defined"

  override def validate(options: String) = {
    val optionsMap = serializer.deserialize[Map[String, String]](options)
    val fieldNames = Seq(ipv4DatField, ipv6DatField)

    val atLeastOneFieldDefined = fieldNames.map { field =>
      optionsMap.get(field).filter(_.length > 0)
    }.exists(_.isDefined)

    if (atLeastOneFieldDefined)
      ValidationInfo(result = false, ArrayBuffer(error))
    else
      ValidationInfo(result = true)
  }
}

object OptionsLiterals {
  val ipv4DatField = "ipv4-dat"
  val ipv6DatField = "ipv6-dat"
}
