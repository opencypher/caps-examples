/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.example.lab1

import java.util.UUID

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

object SparkExample extends App {

  // Start a local Spark session
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName(s"caps-local-${UUID.randomUUID()}")
    .getOrCreate()

  spark.sparkContext.setLogLevel("error")

  // Create some data
  val persons = List(
    Row(0L, "Alice", "Scala", 42L),
    Row(1L, "Bob", "Rust", 23L),
    Row(2L, "Eve", "Scala", 84L),
    Row(2L, "Carol", "Java", 30L)
  ).asJava

  val personSchema = StructType(List(
    StructField("id", LongType, false),
    StructField("name", StringType, false),
    StructField("fav_language", StringType, false),
    StructField("age", LongType, false))
  )

  val personDF = spark.createDataFrame(persons, personSchema)

  // This import is needed to use the $-notation
  import spark.implicits._

  // Perform relational operations on the data
  personDF.select("name").show()

  personDF
    .filter($"age" > 23)
    .select("name", "age")
    .show()

  personDF
    .groupBy($"fav_language")
    .agg(max("age"), avg("age"))
    .show()

  // Create some more data
  val livesIn = List(
    Row(0L, "London"),
    Row(1L, "Leipzig"),
    Row(2L, "Malmö"),
    Row(3L, "San Mateo")
  ).asJava

  val livesInSchema = StructType(List(
    StructField("person_id", LongType, false),
    StructField("city", StringType, false))
  )

  val livesInDF = spark.createDataFrame(livesIn, livesInSchema)

  // Perform some more relational operations on the data
  personDF
    .join(livesInDF, $"id" === $"person_id")
    .select("name", "city")
    .show()
}
