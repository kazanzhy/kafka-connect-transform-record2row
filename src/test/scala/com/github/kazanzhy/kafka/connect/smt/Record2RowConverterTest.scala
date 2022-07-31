package com.github.kazanzhy.kafka.connect.smt

import java.util
import org.scalatest.funsuite.AnyFunSuite
import org.apache.kafka.connect.data.{Field, Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord


object Record2RowConverterTest extends AnyFunSuite {
  val conf = new util.HashMap[String, AnyRef]
  conf.put("json.writer.output.mode", "RELAXED")
  conf.put("json.string.field.name", "")
  conf.put("include.field.names", "")
  conf.put("exclude.field.names", "")

  val primitivesSchema: Schema = SchemaBuilder.struct.name("primitivesSchema").version(1)
      .field("boolean", Schema.BOOLEAN_SCHEMA)
      .field("int8", Schema.INT8_SCHEMA)
      .field("int16", Schema.INT16_SCHEMA)
      .field("int32", Schema.INT32_SCHEMA)
      .field("int64", Schema.INT64_SCHEMA)
      .field("float32", Schema.FLOAT32_SCHEMA)
      .field("float64", Schema.FLOAT64_SCHEMA)
      .field("string", Schema.STRING_SCHEMA)
      .field("bytes", Schema.BYTES_SCHEMA)
      .build

  val primitivesStruct: Struct = new Struct(primitivesSchema)
  primitivesStruct.put("boolean", true)
  primitivesStruct.put("int8", 8.toByte)
  primitivesStruct.put("int16", 2.toShort)
  primitivesStruct.put("int32", 3)
  primitivesStruct.put("int64", 4L)
  primitivesStruct.put("float32", 1.0f)
  primitivesStruct.put("float64", 2.0d)
  primitivesStruct.put("string", "TestString")
  primitivesStruct.put("bytes", "TestBytes".getBytes)

  private val structSchema: Schema = SchemaBuilder.struct.name("structSchema").version(1)
      .field("name", Schema.OPTIONAL_STRING_SCHEMA)
      .field("age", Schema.OPTIONAL_INT8_SCHEMA)
      .build

  private val structStruct = new Struct(structSchema)
  structStruct.put("name", "John")
  structStruct.put("age", 42.toByte)

  val complexesSchema: Schema = SchemaBuilder.struct.name("complexesSchema").version(1)
      .field("array", SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build)
      .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build)
      .field("record", structSchema)
      .build

  val complexesStruct: Struct = new Struct(complexesSchema)
  complexesStruct.put("array", util.List.of(true, false))
  complexesStruct.put("map", util.Map.of("key", "value"))
  complexesStruct.put("record", structStruct)

  val complexesStringSchema: Schema = SchemaBuilder.struct.name("complexesSchema").version(1)
      .field("array", Schema.STRING_SCHEMA)
      .field("map", Schema.STRING_SCHEMA)
      .field("record", Schema.STRING_SCHEMA)
      .build
}

class Record2RowConverterTest extends AnyFunSuite {
  def isStructSchemasEqual(fst: Schema, snd: Schema): Boolean = {
    (fst.fields.toArray zip snd.fields.toArray).forall({
      case (f: Field, s: Field) =>
        f.name === s.name && f.schema.`type` === s.schema.`type`
    })
  }

  def isStructEqual(fst: Struct, snd: Struct): Boolean = {
    val fstFields = fst.schema.fields.toArray.map({case f: Field => f.name})
    val sndFields = snd.schema.fields.toArray.map({case f: Field => f.name})
    val fields = fstFields.toSet.union(sndFields.toSet)
    fields.forall(f => fst.get(f) === snd.get(f))
  }

  def isAllStructFieldsString(struct: Struct): Boolean = {
    val fields = struct.schema.fields.toArray.map({case f: Field => f.name})
    fields.forall(f => struct.get(f).isInstanceOf[String])
  }

  def isAllStringFieldsStartsWith(struct: Struct, patt: String): Boolean = {
    struct.schema.fields.toArray.forall({
      case f: Field => struct.get(f.name).toString.startsWith(patt)
    })
  }
}


class RecordKey2RowConverterTest extends Record2RowConverterTest {
  import Record2RowConverterTest._
  var keySmt = new RecordKey2RowConverter[SinkRecord]
  keySmt.configure(conf)

  test("Record Key: Handle Tombstone Record") {
    val record = new SinkRecord(null, 0, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "test", 0)
    val transformedRecord = keySmt.apply(record)
    assert(transformedRecord.isInstanceOf[SinkRecord])
    assert(transformedRecord.keySchema === Schema.STRING_SCHEMA)
    assert(transformedRecord.key === null)
  }

  test("Record Key: Primitives Transforming") {
    val record = new SinkRecord(null, 0, primitivesSchema, primitivesStruct, Schema.STRING_SCHEMA, "test", 0)
    val transformedRecord = keySmt.apply(record)
    assert(transformedRecord.isInstanceOf[SinkRecord])
    assert(transformedRecord.keySchema.fields.size === 9)
    assert(isStructSchemasEqual(transformedRecord.keySchema, primitivesSchema))
    assert(isStructEqual(transformedRecord.key.asInstanceOf[Struct], primitivesStruct))
  }

  test("Record Key: Complexes Transforming") {
    val record = new SinkRecord(null, 0, complexesSchema, complexesStruct, Schema.STRING_SCHEMA, "test", 0)
    val transformedRecord = keySmt.apply(record)
    assert(transformedRecord.isInstanceOf[SinkRecord])
    assert(transformedRecord.keySchema.fields.size === 3)
    assert(isStructSchemasEqual(transformedRecord.keySchema, complexesStringSchema))
    assert(isAllStructFieldsString(transformedRecord.key.asInstanceOf[Struct]))
  }

  test("Record Value: Writer Mode Parameter") {
    val customConf = new util.HashMap[String, AnyRef]
    customConf.put("json.writer.output.mode", "EXTENDED")
    keySmt.configure(customConf)
    val record = new SinkRecord(null, 0, complexesSchema, complexesStruct, Schema.STRING_SCHEMA, "test", 0)
    val transformedRecord = keySmt.apply(record)
    assert(transformedRecord.key.asInstanceOf[Struct].get("record").toString === "{\"record\": {\"name\": \"John\", \"age\": {\"$numberInt\": \"42\"}}}")
  }

  test("Record Key: Field Name Parameter") {
    val customConf = new util.HashMap[String, AnyRef]
    customConf.put("json.string.field.name", "myfieldname")
    keySmt.configure(customConf)
    val record = new SinkRecord(null, 0, complexesSchema, complexesStruct, Schema.STRING_SCHEMA, "test", 0)
    val transformedRecord = keySmt.apply(record)
    assert(isAllStringFieldsStartsWith(transformedRecord.key.asInstanceOf[Struct], "{\"myfieldname\": "))
  }

  test("Record Key: Include Parameter") {
    val customConf = new util.HashMap[String, AnyRef]
    customConf.put("include.field.names", "boolean,int8,int16,int32,int64,float32,float64,bytes")
    keySmt.configure(customConf)
    val record = new SinkRecord(null, 0, primitivesSchema, primitivesStruct, Schema.STRING_SCHEMA, "test", 0)
    val transformedRecord = keySmt.apply(record)
    assert(isAllStructFieldsString(transformedRecord.key.asInstanceOf[Struct]))
  }

  test("Record Key: Exclude Parameter") {
    val customConf = new util.HashMap[String, AnyRef]
    customConf.put("exclude.field.names", "array,map,record")
    keySmt.configure(customConf)
    val record = new SinkRecord(null, 0, complexesSchema, complexesStruct, Schema.STRING_SCHEMA, "test", 0)
    val transformedRecord = keySmt.apply(record)
    assert(isStructSchemasEqual(transformedRecord.keySchema, complexesSchema))
  }
}

class RecordValue2RowConverterTest extends Record2RowConverterTest {
  import Record2RowConverterTest._
  val valueSmt = new RecordValue2RowConverter[SinkRecord]
  valueSmt.configure(conf)

  test("Record Value: Handle Tombstone Record") {
    val record = new SinkRecord(null, 0, Schema.STRING_SCHEMA, "test", Schema.STRING_SCHEMA, null,  0)
    val transformedRecord = valueSmt.apply(record)
    assert(transformedRecord.isInstanceOf[SinkRecord])
    assert(transformedRecord.valueSchema === Schema.STRING_SCHEMA)
    assert(transformedRecord.value === null)
  }

  test("Record Value: Primitives Transforming") {
    val record = new SinkRecord(null, 0, Schema.STRING_SCHEMA, "test", primitivesSchema, primitivesStruct,  0)
    val transformedRecord = valueSmt.apply(record)
    assert(transformedRecord.isInstanceOf[SinkRecord])
    assert(transformedRecord.valueSchema.fields.size === 9)
    assert(isStructSchemasEqual(transformedRecord.valueSchema, primitivesSchema))
    assert(isStructEqual(transformedRecord.value.asInstanceOf[Struct], primitivesStruct))
  }

  test("Record Value: Complexes Transforming") {
    val record = new SinkRecord(null, 0, Schema.STRING_SCHEMA, "test", complexesSchema, complexesStruct,  0)
    val transformedRecord = valueSmt.apply(record)
    assert(transformedRecord.isInstanceOf[SinkRecord])
    assert(transformedRecord.valueSchema.fields.size === 3)
    assert(isStructSchemasEqual(transformedRecord.valueSchema, complexesStringSchema))
    assert(isAllStructFieldsString(transformedRecord.value.asInstanceOf[Struct]))
  }

  test("Record Value: Writer Mode Parameter") {
    val customConf = new util.HashMap[String, AnyRef]
    customConf.put("json.writer.output.mode", "EXTENDED")
    valueSmt.configure(customConf)
    val record = new SinkRecord(null, 0, Schema.STRING_SCHEMA, "test", complexesSchema, complexesStruct, 0)
    val transformedRecord = valueSmt.apply(record)
    assert(transformedRecord.value.asInstanceOf[Struct].get("record").toString === "{\"record\": {\"name\": \"John\", \"age\": {\"$numberInt\": \"42\"}}}")
  }

  test("Record Value: Field Name Parameter") {
    val customConf = new util.HashMap[String, AnyRef]
    customConf.put("json.string.field.name", "myfieldname")
    valueSmt.configure(customConf)
    val record = new SinkRecord(null, 0, Schema.STRING_SCHEMA, "test", complexesSchema, complexesStruct,  0)
    val transformedRecord = valueSmt.apply(record)
    assert(isAllStringFieldsStartsWith(transformedRecord.value.asInstanceOf[Struct], "{\"myfieldname\": "))
  }

  test("Record Value: Include Parameter") {
    val customConf = new util.HashMap[String, AnyRef]
    customConf.put("include.field.names", "boolean,int8,int16,int32,int64,float32,float64,bytes")
    valueSmt.configure(customConf)
    val record = new SinkRecord(null, 0, Schema.STRING_SCHEMA, "test", primitivesSchema, primitivesStruct,  0)
    val transformedRecord = valueSmt.apply(record)
    assert(isAllStructFieldsString(transformedRecord.value.asInstanceOf[Struct]))
  }

  test("Record Value: Exclude Parameter") {
    val customConf = new util.HashMap[String, AnyRef]
    customConf.put("exclude.field.names", "array,map,record")
    valueSmt.configure(customConf)
    val record = new SinkRecord(null, 0, Schema.STRING_SCHEMA, "test", complexesSchema, complexesStruct,  0)
    val transformedRecord = valueSmt.apply(record)
    assert(isStructSchemasEqual(transformedRecord.valueSchema, complexesSchema))
  }
}
