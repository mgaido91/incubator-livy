/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy.thriftserver.utils

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.serde2.thrift.Type
import org.apache.hive.service.cli.TableSchema
import org.json4s.{DefaultFormats, JValue}
import org.json4s.JsonAST.{JObject, JString}
import org.json4s.jackson.JsonMethods.parse

import org.apache.livy.Logging

object Types extends Logging {
  // Used for JSON conversion
  private implicit val formats = DefaultFormats

  private def toHive(jValue: JValue): String = {
    jValue match {
      case JString(t) => primitiveToHive(t)
      case o: JObject => complexToHive(o)
      case _ => throw new IllegalArgumentException(
        s"Spark type was neither a string nor a object. It was: $jValue.")
    }
  }

  private def getMainType(jValue: JValue): String = {
    jValue match {
      case JString(t) => t
      case o: JObject => (o \ "type").extract[String]
      case _ => throw new IllegalArgumentException(
        s"Spark type was neither a string nor a object. It was: $jValue.")
    }
  }

  private def primitiveToHive(sparkType: String): String = {
    sparkType match {
      case "integer" => "int"
      case "long" => "bigint"
      case "short" => "smallint"
      case "byte" => "tinyint"
      case "null" => "void"
      // boolean, string, float, double, decimal, date, timestamp are the same
      case other => other
    }
  }

  private def complexToHive(sparkType: JObject): String = {
    (sparkType \ "type").extract[String] match {
      case "array" => s"array<${toHive(sparkType \ "elementType")}>"
      case "struct" =>
        val fields = (sparkType \ "fields").children.map { f =>
          s"${(f \ "name").extract[String]}:${toHive(f \ "type")}"
        }
        s"struct<${fields.mkString(",")}>"
      case "map" => s"map<${toHive(sparkType \ "keyType")}, ${toHive(sparkType \ "valueType")}>"
      case "udt" => toHive(sparkType \ "sqlType")
    }
  }

  def tableSchemaFromSparkJson(sparkJson: String): TableSchema = {
    debug(s"Parsing Spark schema into Hive's one: $sparkJson.")
    val schema = parse(sparkJson) \ "fields"
    val fields = schema.children.map { field =>
      val name = (field \ "name").extract[String]
      val hiveType = toHive(field \ "type")
      new FieldSchema(name, hiveType, "")
    }
    new TableSchema(fields.asJava)
  }

  def getMainTypes(sparkJson: String): Array[String] = {
    debug(s"Get main types for: $sparkJson.")
    val schema = parse(sparkJson) \ "fields"
    schema.children.map { field =>
      getMainType(field \ "type")
    }.toArray
  }

  def toThriftType(sparkDataType: String): Type = {
    sparkDataType match {
      case "boolean" => Type.BOOLEAN_TYPE
      case "byte" => Type.TINYINT_TYPE
      case "short" => Type.SMALLINT_TYPE
      case "integer" => Type.INT_TYPE
      case "long" => Type.BIGINT_TYPE
      case "float" => Type.FLOAT_TYPE
      case "double" => Type.DOUBLE_TYPE
      case "binary" => Type.BINARY_TYPE
      case "null" => Type.NULL_TYPE
      case _ => Type.STRING_TYPE
    }
  }
}
