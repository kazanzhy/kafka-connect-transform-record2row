package com.github.kazanzhy.kafka.connect.smt

import mjson.Json
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.errors.DataException
import org.scalatest.funsuite.AnyFunSuite

import java.nio.ByteBuffer
import java.util.{Date => JUDate}
import java.math.{BigDecimal => JUBigDecimal}
import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}


class Connect2JsonConverterTest extends AnyFunSuite {
  test("Converts null value") {
    assert(Connect2JsonConverter.convert(null, Schema.OPTIONAL_BOOLEAN_SCHEMA) == Json.nil)
  }

  test("Converts boolean value") {
    assert(Connect2JsonConverter.convert(true, Schema.BOOLEAN_SCHEMA) == Json.make(true))
  }

  test("Converts int8 value") {
    assert(Connect2JsonConverter.convert(42.toByte, Schema.INT8_SCHEMA) == Json.make(42))
  }

  test("Converts int16 value") {
    assert(Connect2JsonConverter.convert(42.toShort, Schema.INT16_SCHEMA) == Json.make(42))
  }

  test("Converts int32 value") {
    assert(Connect2JsonConverter.convert(42, Schema.INT32_SCHEMA) == Json.make(42))
  }

  test("Converts int64 value") {
    assert(Connect2JsonConverter.convert(42L, Schema.INT64_SCHEMA) == Json.make(42))
  }

  test("Converts float32 value") {
    assert(Connect2JsonConverter.convert(3.14f, Schema.FLOAT32_SCHEMA) == Json.make(3.14f))
  }

  test("Converts float64 value") {
    assert(Connect2JsonConverter.convert(3.14d, Schema.FLOAT64_SCHEMA) == Json.make(3.14))
  }

  test("Converts string value") {
    assert(Connect2JsonConverter.convert("foobar", Schema.STRING_SCHEMA) == Json.make("foobar"))
  }

  test("Converts ByteBuffer to Base64-encoded string") {
    assert(Connect2JsonConverter.convert(ByteBuffer.wrap(Array(4.toByte, 2.toByte)), Schema.BYTES_SCHEMA) == Json.make("BAI="))
  }

  test("Converts byte array to Base64-encoded string") {
    assert(Connect2JsonConverter.convert(Array(4.toByte, 2.toByte), Schema.BYTES_SCHEMA) == Json.make("BAI="))
  }

  test("Throws exception for unknown bytes type") {
    val caught = intercept[DataException](Connect2JsonConverter.convert("test".getBytes.toList, Schema.BYTES_SCHEMA))
    assert(caught.getCause.getMessage == "error: bytes field conversion failed to due unexpected object type scala.collection.immutable.$colon$colon")
  }

  test("Converts struct to JSON object") {
    val schema = SchemaBuilder.struct.name("primitivesSchema").version(1)
      .field("boolean", Schema.BOOLEAN_SCHEMA)
      .field("int8", Schema.INT8_SCHEMA)
      .field("int16", Schema.INT16_SCHEMA)
      .field("int32", Schema.INT32_SCHEMA)
      .field("int64", Schema.INT64_SCHEMA)
      .field("float32", Schema.FLOAT32_SCHEMA)
      .field("float64", Schema.FLOAT64_SCHEMA)
      .field("string", Schema.STRING_SCHEMA)
      .build

    val struct = new Struct(schema)
      .put("boolean", true)
      .put("int8", 8.toByte)
      .put("int16", 2.toShort)
      .put("int32", 3)
      .put("int64", 4L)
      .put("float32", 1.0f)
      .put("float64", 2.0d)
      .put("string", "TestString")

    assert(
      Connect2JsonConverter.convert(struct, schema) ==
        Json.`object`()
          .set("boolean", true)
          .set("int8", 8)
          .set("int16", 2)
          .set("int32", 3)
          .set("int64", 4)
          .set("float32", 1f)
          .set("float64", 2d)
          .set("string", "TestString")
    )
  }

  test("Converts array to JSON array") {
    val schema = SchemaBuilder.array(Schema.STRING_SCHEMA).build

    assert(Connect2JsonConverter.convert(List("foo", "bar").asJava, schema) == Json.array("foo", "bar"))
  }

  test("Converts map to JSON object") {
    val schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build

    assert(Connect2JsonConverter.convert(Map("foo" -> "bar", "baz" -> "bat").asJava, schema) == Json.`object`().set("foo", "bar").set("baz", "bat"))
  }

  test("Converts complex nested structure") {
    val nestedSchema = SchemaBuilder.struct.name("nested").version(1)
      .field("array", SchemaBuilder.array(Schema.INT32_SCHEMA).build)
      .field("bytes", Schema.BYTES_SCHEMA)
      .build
    val mapValueSchema = SchemaBuilder.struct.name("mapValue").version(1)
      .field("int", Schema.OPTIONAL_INT32_SCHEMA)
      .build
    val schema = SchemaBuilder.struct.name("complex").version(1)
      .field("string", Schema.STRING_SCHEMA)
      .field("nestedStruct", nestedSchema)
      .field("map", SchemaBuilder.map(Schema.INT32_SCHEMA, mapValueSchema).build)
      .build

    val struct = new Struct(schema)
      .put("string", "foobar")
      .put(
        "nestedStruct",
        new Struct(nestedSchema)
          .put("array", List(4, 2).asJava)
          .put("bytes", "test".getBytes)
      )
      .put(
        "map",
        Map(
          1 -> new Struct(mapValueSchema).put("int", 3),
          2 -> new Struct(mapValueSchema).put("int", null)
        ).asJava
      )

    assert(
      Connect2JsonConverter.convert(struct, schema) ==
        Json.`object`()
          .set("string", "foobar")
          .set(
            "nestedStruct",
            Json.`object`()
              .set("array", Json.array(4, 2))
              .set("bytes", "dGVzdA==")
          )
          .set(
            "map",
            Json.`object`()
              .set("1", Json.`object`().set("int", 3))
              .set("2", Json.`object`().set("int", null))
          )
    )
  }

  test("Converts logical date to ISO date string") {
    assert(Connect2JsonConverter.convert(new JUDate(1661126400000L), Date.SCHEMA) == Json.make("2022-08-22"))
  }

  test("Converts logical time to ISO time string") {
    assert(Connect2JsonConverter.convert(new JUDate(11655000L), Time.SCHEMA) == Json.make("03:14:15"))
  }

  test("Converts logical timestamp to ISO timestamp") {
    assert(Connect2JsonConverter.convert(new JUDate(1661138055000L), Timestamp.SCHEMA) == Json.make("2022-08-22T03:14:15Z"))
  }

  test("Converts logical decimal to JSON number") {
    assert(Connect2JsonConverter.convert(new JUBigDecimal("4.2"), Decimal.schema(2)) == Json.make(JUBigDecimal.valueOf(420, 2)))
  }

  test("Wraps unexpected exception") {
    val caught = intercept[DataException](Connect2JsonConverter.convert("test", Schema.BOOLEAN_SCHEMA))
    assert(caught.getMessage == "error while processing BOOLEAN value")
  }
}
