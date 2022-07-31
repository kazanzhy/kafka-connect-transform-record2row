package com.github.kazanzhy.kafka.connect.smt

import scala.jdk.CollectionConverters._
import java.util
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.{SimpleConfig, Requirements}
import org.apache.logging.log4j.scala.Logging
import org.bson.json.{JsonMode, JsonWriterSettings}
import at.grahsl.kafka.connect.mongodb.converter.AvroJsonSchemafulRecordConverter


object Record2RowConverter extends Logging {
  final val OVERVIEW_DOC = "Converts a not primitive fields into a JSON string fields."
  private val PURPOSE = "Converting record with Schema into a record with a new schema."

  private object ConfigName {
    val JSON_STRING_FIELD_NAME = "json.string.field.name"
    val JSON_WRITER_OUTPUT_MODE = "json.writer.output.mode"
    val INCLUDE_FIELD_NAMES = "include.field.names"
    val EXCLUDE_FIELD_NAMES = "exclude.field.names"
  }

  val CONFIG_DEF: ConfigDef = new ConfigDef()
      .define(
        ConfigName.JSON_WRITER_OUTPUT_MODE,
        ConfigDef.Type.STRING, "RELAXED",
        ConfigDef.Importance.MEDIUM, "Output mode of JSON Writer (RELAXED, EXTENDED, SHELL)")
      .define(
        ConfigName.JSON_STRING_FIELD_NAME,
        ConfigDef.Type.STRING, "",
        ConfigDef.Importance.HIGH, "Field name for output JSON String field")
      .define(
        ConfigName.INCLUDE_FIELD_NAMES,
        ConfigDef.Type.STRING, "",
        ConfigDef.Importance.MEDIUM, "Names of primitive fields to forcibly convert to JSON String")
      .define(
        ConfigName.EXCLUDE_FIELD_NAMES,
        ConfigDef.Type.STRING, "",
        ConfigDef.Importance.MEDIUM, "Names of complex fields to keep original data type")
}


sealed abstract class Record2RowConverter[R <: ConnectRecord[R]] extends Transformation[R] with Logging {
  private var converter: AvroJsonSchemafulRecordConverter = null
  private var jsonWriterSettings: JsonWriterSettings = null
  private var jsonStringFieldName: String = null
  private var includeFieldNames: String = null
  private var excludeFieldNames: String = null

  protected def operatingSchema(record: R): Schema
  protected def operatingValue(record: R): Object
  protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object): R

  def config(): ConfigDef = {
    Record2RowConverter.CONFIG_DEF
  }

  private def toJsonMode(jsonMode: String) = jsonMode match {
    case "SHELL" => JsonMode.SHELL
    case "EXTENDED" => JsonMode.EXTENDED
    case _ => JsonMode.RELAXED
  }

  def configure(props: util.Map[String, _]): Unit = {
    val conf: SimpleConfig = new SimpleConfig(Record2RowConverter.CONFIG_DEF, props)
    converter = new AvroJsonSchemafulRecordConverter()
    jsonWriterSettings = JsonWriterSettings.builder
        .outputMode(toJsonMode(conf.getString(Record2RowConverter.ConfigName.JSON_WRITER_OUTPUT_MODE)))
        .build
    jsonStringFieldName = conf.getString(Record2RowConverter.ConfigName.JSON_STRING_FIELD_NAME)
    includeFieldNames = conf.getString(Record2RowConverter.ConfigName.INCLUDE_FIELD_NAMES)
    excludeFieldNames = conf.getString(Record2RowConverter.ConfigName.EXCLUDE_FIELD_NAMES)
  }

  private def getFieldNamesToConvert(schema: Schema): Set[String] = {
    val include = includeFieldNames.split(",").toSet
    val exclude = excludeFieldNames.split(",").toSet
    val default = schema.fields.asScala.filterNot(_.schema.`type`.isPrimitive).map(_.name).toSet
    default ++ include -- exclude
  }

  private def makeOutputSchema(inputSchema: Schema, fieldsToConvert: Set[String]): Schema = {
    val outputSchemaBuilder = SchemaBuilder.struct.name("outputSchema").version(1)
    for (field <- inputSchema.fields.asScala){
      if (fieldsToConvert.contains(field.name)) {
        outputSchemaBuilder.field(field.name, Schema.STRING_SCHEMA)
      } else {
        outputSchemaBuilder.field(field.name, field.schema)
      }
    }
    outputSchemaBuilder.build()
  }

  private def makeOutputValue(inputValue: Struct, inputSchema: Schema, outputSchema: Schema, fieldsToConvert: Set[String]): Object = {
    val outputValue = new Struct(outputSchema)
    for (field <- inputSchema.fields.asScala){
      if (fieldsToConvert.contains(field.name)) {
        val newFieldName = if (jsonStringFieldName == "") field.name else jsonStringFieldName
        val newFieldSchema = SchemaBuilder.struct.field(newFieldName, field.schema).build()
        val newFieldValue = new Struct(newFieldSchema)
        newFieldValue.put(newFieldName, inputValue.get(field.name))
        val bsonDoc = converter.convert(newFieldSchema, newFieldValue)
        val jsonDoc = bsonDoc.toJson(jsonWriterSettings)
        outputValue.put(field.name, jsonDoc)
      } else {
        outputValue.put(field.name, inputValue.get(field.name))
      }
    }
    outputValue
  }

  private def modifyRecord(record: R): R = {
    val inputSchema = operatingSchema(record)
    val inputValue = Requirements.requireStruct(operatingValue(record), Record2RowConverter.PURPOSE)
    val fieldsToConvert = getFieldNamesToConvert(inputSchema)
    val outputSchema = makeOutputSchema(inputSchema, fieldsToConvert)
    val outputValue = makeOutputValue(inputValue, inputSchema, outputSchema, fieldsToConvert)
    newRecord(record, outputSchema, outputValue)
  }

  def apply(record: R): R = {
    if (operatingValue(record) == null) {
      record
    } else if (operatingSchema(record) == null) {
      Record2RowConverter.logger.info("Transformation is ignoring value/key without schema")
      record
    } else {
      modifyRecord(record)
    }
  }

  def close(): Unit = {
    converter = null
  }
}

final class RecordKey2RowConverter[R <: ConnectRecord[R]] extends Record2RowConverter[R] {
  override protected def operatingSchema(record: R): Schema = record.keySchema
  override protected def operatingValue(record: R): Object = record.key
  override protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object): R = {
    record.newRecord(
      record.topic, record.kafkaPartition,
      updatedSchema, updatedValue,
      record.valueSchema, record.value,
      record.timestamp
    )
  }

}

final class RecordValue2RowConverter[R <: ConnectRecord[R]] extends Record2RowConverter[R] {
  override protected def operatingSchema(record: R): Schema = record.valueSchema
  override protected def operatingValue(record: R): Object = record.value
  override protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object): R = {
    record.newRecord(
      record.topic, record.kafkaPartition,
      record.keySchema, record.key,
      updatedSchema, updatedValue,
      record.timestamp
    )
  }
}