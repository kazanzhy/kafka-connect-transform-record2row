package com.github.kazanzhy.kafka.connect.smt

import org.apache.kafka.connect.data._
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters.MapHasAsJava


class BsonWriterTest extends AnyFunSuite {
  private val writer = new BsonWriter
  writer.configure(Map[String, Any]().asJava)

  test("Wraps value in struct with original field name") {
    assert(writer.write(new Field("foo", 0, Schema.STRING_SCHEMA), "bar") == """{"foo": "bar"}""")
  }

  test("Wraps value in struct with configured field name if provided") {
    writer.configure(Map("json.string.field.name" -> "value").asJava)
    assert(writer.write(new Field("foo", 0, Schema.STRING_SCHEMA), "bar") == """{"value": "bar"}""")
  }

  test("Uses extended output mode if configured") {
    writer.configure(Map("json.writer.output.mode" -> "EXTENDED").asJava)
    assert(writer.write(new Field("foo", 0, Schema.INT64_SCHEMA), 42L) == """{"foo": {"$numberLong": "42"}}""")
  }
}
