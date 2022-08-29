package com.github.kazanzhy.kafka.connect.smt

import at.grahsl.kafka.connect.mongodb.converter.AvroJsonSchemafulRecordConverter
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.data.{Field, SchemaBuilder, Struct}
import org.apache.kafka.connect.transforms.util.SimpleConfig
import org.bson.json.{JsonMode, JsonWriterSettings}

import java.util

object BsonWriter {
  private object ConfigName {
    val JSON_STRING_FIELD_NAME = "json.string.field.name"
    val JSON_WRITER_OUTPUT_MODE = "json.writer.output.mode"
  }

  val CONFIG_DEF = new ConfigDef()
    .define(
      ConfigName.JSON_WRITER_OUTPUT_MODE,
      ConfigDef.Type.STRING, "RELAXED",
      ConfigDef.Importance.MEDIUM, "Output mode of JSON Writer (RELAXED, EXTENDED, SHELL)")
    .define(
      ConfigName.JSON_STRING_FIELD_NAME,
      ConfigDef.Type.STRING, "",
      ConfigDef.Importance.HIGH, "Field name for output JSON String field")
}

class BsonWriter extends JsonWriter {
  private var converter: AvroJsonSchemafulRecordConverter = null
  private var jsonWriterSettings: JsonWriterSettings = null
  private var jsonStringFieldName: String = null

  private def toJsonMode(jsonMode: String) = jsonMode match {
    case "SHELL" => JsonMode.SHELL
    case "EXTENDED" => JsonMode.EXTENDED
    case _ => JsonMode.RELAXED
  }

  override def configure(props: util.Map[String, _]): Unit = {
    val conf: SimpleConfig = new SimpleConfig(BsonWriter.CONFIG_DEF, props)
    converter = new AvroJsonSchemafulRecordConverter()
    jsonWriterSettings = JsonWriterSettings.builder
      .outputMode(toJsonMode(conf.getString(BsonWriter.ConfigName.JSON_WRITER_OUTPUT_MODE)))
      .build
    jsonStringFieldName = conf.getString(BsonWriter.ConfigName.JSON_STRING_FIELD_NAME)
  }

  override def write(field: Field, value: Any): String = {
    val newFieldName = if (jsonStringFieldName == "") field.name else jsonStringFieldName
    val newFieldSchema = SchemaBuilder.struct.field(newFieldName, field.schema).build()
    val newFieldValue = new Struct(newFieldSchema)
    newFieldValue.put(newFieldName, value)
    val bsonDoc = converter.convert(newFieldSchema, newFieldValue)

    bsonDoc.toJson(jsonWriterSettings)
  }
}
