package com.github.kazanzhy.kafka.connect.smt

import mjson.Json
import org.apache.kafka.connect.data.Schema.Type._
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.errors.DataException

import java.math.{BigDecimal => JUBigDecimal}
import java.nio.ByteBuffer
import java.time.{Instant, ZoneOffset}
import java.util.{Base64, Date => JUDate}
import scala.collection.mutable
import scala.jdk.CollectionConverters.{ListHasAsScala, MapHasAsScala}

trait LogicalTypeConverter {
  def convert(factory: Json.Factory, value: Any, schema: Schema): Json
}

class DateConverter(mapToString: Instant => String) extends LogicalTypeConverter {
  override def convert(factory: Json.Factory, value: Any, schema: Schema): Json =
    factory.string(mapToString(Instant.ofEpochMilli(value.asInstanceOf[JUDate].getTime)))
}

object DecimalConverter extends LogicalTypeConverter {
  override def convert(factory: Json.Factory, value: Any, schema: Schema): Json =
    factory.number(value.asInstanceOf[JUBigDecimal])
}

object Connect2JsonConverter {
  private val factory = Json.factory()
  private val base64 = Base64.getEncoder
  private val logicalTypeConverters = Map(
    Date.LOGICAL_NAME -> new DateConverter(_.atOffset(ZoneOffset.UTC).toLocalDate.toString),
    Time.LOGICAL_NAME -> new DateConverter(_.atOffset(ZoneOffset.UTC).toLocalTime.toString),
    Timestamp.LOGICAL_NAME -> new DateConverter(_.toString),
    Decimal.LOGICAL_NAME -> DecimalConverter
  )

  def convert(value: Any, schema: Schema): Json = {
    if (value == null) {
      return Json.nil
    }

    try {
      logicalTypeConverters.get(schema.name)
        .map(_.convert(factory, value, schema))
        .getOrElse(
          schema.`type` match {
            case BOOLEAN => factory.bool(value.asInstanceOf[Boolean])
            case INT8 | INT16 | INT32 | INT64 | FLOAT32 | FLOAT64 => factory.number(value.asInstanceOf[Number])
            case STRING => factory.string(value.asInstanceOf[String])
            case BYTES => factory.string(
              base64.encodeToString(
                value match {
                  case bb: ByteBuffer => bb.array()
                  case ba: Array[Byte] => ba
                  case _ => throw new DataException(s"error: bytes field conversion failed to due unexpected object type ${value.getClass.getName}")
                }
              )
            )
            case STRUCT => convertStruct(value.asInstanceOf[Struct], schema)
            case ARRAY => convertArray(value.asInstanceOf[java.util.List[Any]].asScala, schema)
            case MAP => convertMap(value.asInstanceOf[java.util.Map[Any, Any]].asScala, schema)
          }
        )
    } catch {
      case e: Exception => throw new DataException(s"error while processing ${describeSchema(schema)} value", e)
    }
  }

  private def convertStruct(struct: Struct, schema: Schema) =
    schema.fields.asScala.foldLeft(Json.`object`())(
      (acc, field) => try {
        acc.set(field.name, convert(struct.get(field), field.schema))
      } catch {
        case e: Exception => throw new DataException(s"error while processing ${describeSchema(field.schema)} field '${field.name}'", e)
      }
    )

  private def convertArray(list: mutable.Buffer[Any], schema: Schema) =
    list.foldLeft(Json.array())(
      (acc, value) => try {
        acc.add(convert(value, schema.valueSchema))
      } catch {
        case e: Exception => throw new DataException(s"error while processing ${describeSchema(schema.valueSchema)} array value", e)
      }
    )

  private def convertMap(map: mutable.Map[Any, Any], schema: Schema) =
    map.foldLeft(Json.`object`()) {
      case (acc, (key, value)) =>
        try {
          acc.set(key.toString, convert(value, schema.valueSchema))
        } catch {
          case e: Exception => throw new DataException(s"error while processing ${describeSchema(schema.valueSchema)} map field $key", e)
        }
    }

  private def describeSchema(schema: Schema) = if (schema.name == null) schema.`type`().name
}
