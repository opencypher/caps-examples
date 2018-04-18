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

import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}

/**
  * Demonstrates basic usage of the CAPS API by loading an example network from existing [[DataFrame]]s and
  * running a Cypher query on it.
  */
object DataFrameInputExample extends App {
  // 1) Create CAPS session and retrieve Spark session
  implicit val session: CAPSSession = CAPSSession.local()
  val spark = session.sparkSession

  // 2) Generate some DataFrames that we'd like to interpret as a property graph.
  val nodesDF: DataFrame = ???
  val relsDF: DataFrame = ???

  // 3) Generate node- and relationship scans that wrap the DataFrames and describe their contained data
  val nodeMapping: NodeMapping = ???
  val relMapping: RelationshipMapping = ???

  val nodeTable = CAPSNodeTable(nodeMapping, nodesDF)
  val relTable = CAPSRelationshipTable(relMapping, relsDF)

  // 4) Create property graph from graph scans
  val graph = session.readFrom(nodeTable, relTable)

  // 5) Execute Cypher query and print results
  val result = graph.cypher("MY QUERY STRING")

  result.show

  /**
    * Lab challenge:
    *  - define your own DataFrames with some node and relationship data (possibly more than two tables)
    *  - define entity mappings for the DataFrames
    *  - create a graph from the the Entity Tables
    *  - run a custom query
    */
}
