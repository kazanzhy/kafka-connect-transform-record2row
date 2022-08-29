package com.github.kazanzhy.kafka.connect.smt

import org.apache.kafka.common.config.{ConfigDef, ConfigException}
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.{Field, Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.{Requirements, SimpleConfig}
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.jdk.CollectionConverters._

trait JsonWriter {
  def configure(props: util.Map[String, _]): Unit

  def write(field: Field, value: Any): String
}

object Record2RowConverter {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  final val OVERVIEW_DOC = "Converts a not primitive fields into a JSON string fields."
  private val PURPOSE = "Converting record with Schema into a record with a new schema."

  private object ConfigName {
    val JSON_WRITER = "json.writer"
    val INCLUDE_FIELD_NAMES = "include.field.names"
    val EXCLUDE_FIELD_NAMES = "exclude.field.names"
  }

  val CONFIG_DEF: ConfigDef = new ConfigDef()
      .define(
        ConfigName.JSON_WRITER,
        ConfigDef.Type.STRING, "bson",
        ConfigDef.Importance.HIGH, "Writer to use for converting values to JSON (bson, mjson)")
      .define(
        ConfigName.INCLUDE_FIELD_NAMES,
        ConfigDef.Type.STRING, "",
        ConfigDef.Importance.MEDIUM, "Names of primitive fields to forcibly convert to JSON String")
      .define(
        ConfigName.EXCLUDE_FIELD_NAMES,
        ConfigDef.Type.STRING, "",
        ConfigDef.Importance.MEDIUM, "Names of complex fields to keep original data type")
}


sealed abstract class Record2RowConverter[R <: ConnectRecord[R]] extends Transformation[R] {
  private var jsonWriter: JsonWriter = null
  private var includeFieldNames: String = null
  private var excludeFieldNames: String = null

  protected def operatingSchema(record: R): Schema
  protected def operatingValue(record: R): Object
  protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Object): R

  def config(): ConfigDef = {
    Record2RowConverter.CONFIG_DEF
  }

  def configure(props: util.Map[String, _]): Unit = {
    val conf: SimpleConfig = new SimpleConfig(Record2RowConverter.CONFIG_DEF, props)
    val jsonWriterConfig = conf.getString(Record2RowConverter.ConfigName.JSON_WRITER).toLowerCase
    jsonWriter = jsonWriterConfig match {
      case "bson" => new BsonWriter
      case "mjson" => new MJsonWriter
      case _ => throw new ConfigException(s"Unknown JSON Writer '$jsonWriterConfig' configured")
    }
    jsonWriter.configure(props)
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
        outputValue.put(field.name, jsonWriter.write(field, inputValue.get(field)))
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

  def close(): Unit = ()
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
