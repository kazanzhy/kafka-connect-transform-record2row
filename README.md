# kafka-connect-transform-record2row 
This is a very simple Kafka Connect SMT which takes the entire key or value record and transforms it 
to a new record which contains only primitive types.

## Use Cases
The reason why this SMT was built is the known limitation of the JDBC Sink Connector to handle 
struct, map and arrays that contains not primitive types, like maps or structs. 
All these fields are converted into a JSON String.
But for sure there are other use cases out there where this SMT might be helpful.
Avro types matching with Postgres types using JDBC Sink Connector with or without SMT.

|        Avro type         | JDBCSink <br/>+ Postgres | JDBCSink + SMT <br/>+ Postgres |
|:------------------------:|:------------------------:|:------------------------------:|
|  **_primitive types_**   |                          |                                |
|          `null`          |          `NULL`          |             `NULL`             |
|        `boolean`         |          `BOOL`          |             `BOOL`             |
|          `int`           |          `INT4`          |             `INT4`             |
|          `long`          |          `INT8`          |             `INT8`             |
|         `float`          |         `FLOAT4`         |            `FLOAT4`            |
|         `double`         |         `FLOAT8`         |            `FLOAT8`            |
|         `bytes`          |         `BYTEA`          |            `BYTEA`             |
|         `string`         |          `TEXT`          |             `TEXT`             |
|   **_logical types_**    |                          |                                | 
|        `decimal`         |        `NUMERIC`         |           `NUMERIC`            |
|          `uuid`          |          `TEXT`          |             `TEXT`             |
|          `date`          |          `DATE`          |             `DATE`             |
|      `time-millis`       |          `TIME`          |             `TIME`             |
|      `time-micros`       |          `INT8`          |             `INT8`             |
|    `timestamp-millis`    |       `TIMESTAMP`        |          `TIMESTAMP`           |
|    `timestamp-micros`    |          `INT8`          |             `INT8`             |
| `local-timestamp-millis` |          `INT8`          |             `INT8`             |
| `local-timestamp-micros` |          `INT8`          |             `INT8`             |
|        `duration`        |           ???            |              ???               |
|   **_complex types_**    |                          |                                |
|         `fixed`          |         `BYTEA`          |            `BYTEA`             |
|          `enum`          |          `TEXT`          |             `TEXT`             |
|  `array` of primitives   |       `ARRAY[]`**        |             `TEXT`             |
|    `array` of records    |       _**error**_        |             `TEXT`             |
|         `record`         |       _**error**_        |             `TEXT`             |
|          `map`           |       _**error**_        |             `TEXT`             |
** MySQL and Java DB currently [do not support](https://docs.oracle.com/javase/tutorial/jdbc/basics/array.html) the ARRAY SQL data type.

## Restrictions
This SMT was built to transform Records **with a Schema** to a new Record with a Schema.
So this SMT does not work for Schemaless records.
It was only tested with Avro Schemas backed by Confluent Schema Registry.

## Configuration
```json5
{
  ////
  "transforms": "torow",
  "transforms.torow.type": "com.github.kazanzhy.kafka.connect.smt.RecordValue2RowConverter"
  ////
}
```

### Parameters
|          Name           |                         Description                          |  Type  |             Default             |                    Valid Values                     | Importance |
|:-----------------------:|:------------------------------------------------------------:|:------:|:-------------------------------:|:---------------------------------------------------:|:----------:|
|          type           |                  Class that is used for SMT                  | string |                                 | RecordValue2RowConverter<br/>RecordKey2RowConverter |    high    |
|   include.field.names   | Names of primitive fields to forcibly convert to JSON String | string |  Complex data type field names  |             comma-separates field names             |    high    |
|   exclude.field.names   |      Names of complex fields to keep original data type      | string | Primitive data type field names |             comma-separates field names             |    high    |


## Example
You can run the prepared example in [examples repo](https://gitlab.com/kazanzhy/kafka-connect-smt-examples).

##### Input
* Schema (avro syntax)
```json5
{
  "type": "record",
  "name": "MyEntity",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "subElements", "type": {"type": "array",	"items": {"type": "record","name": "element", "fields": [
      {"name": "id", "type": "string"}
    ]}}}
  ]
}
```

##### Output
* Schema
```json5
{
  "type": "record",
  "name": "MyEntity",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "subElements", "type": "string"}
  ]
}
```

## Build, installation / deployment
There are two ways to use this SMT.
1. Build this project from sources via sbt. Run the following commands in sbt shell:  
  ``sbt clean reload update compile test assembly``  
  As the result you will get uber-jar ``target/kafka-connect-transform-record2row-assembly-x.y.z.jar``.  
  Or just download a pre-built jar from the
  [Releases](https://github.com/kazanzhy/kafka-connect-transform-record2row/actions).  
  For deploy just put it to the place where Kafka Connect is looking for plugins (``plugin.path``).

2. If you're using Confluent Platform image you can build the plugin:
    ```
    sbt clean reload update compile test pack
    mkdir -p target/kafka-connect-transform-record2row/doc
    cp LICENSE target/kafka-connect-transform-record2row/doc/
    cp manifest.json target/kafka-connect-transform-record2row/
    zip -r target/kafka-connect-transform-record2row target{.zip,}
    ```
   Or just download a pre-built plugin from  the 
   [Releases](https://github.com/kazanzhy/kafka-connect-transform-record2row/releases).  
   To install the plugin put archive to yout instance and run:  
   ``confluent-hub install kafka-connect-transform-record2row-x.y.z.zip``

## Acknowledgement
* This project is based on [kafka-connect-transform-tojsonstring](https://github.com/an0r0c/kafka-connect-transform-tojsonstring/)
* Basic structure is from [kafka-connect-insert-uuid](https://github.com/cjmatta/kafka-connect-insert-uuid)
* Transforming ConnectRecord into a Json Document is from [kafka-connect-mongodb](https://github.com/hpgrahsl/kafka-connect-mongodb) 

## License Information

This project is licensed according to [Apache License Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
See the [LICENSE](LICENSE).
