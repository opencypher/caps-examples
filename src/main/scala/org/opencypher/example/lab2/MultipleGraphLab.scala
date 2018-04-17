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
package org.opencypher.example.lab2

import org.opencypher.example.lab2.Neo4jHelpers._
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.neo4j.CommunityNeo4jGraphDataSource

/**
  * The goal of this lab is to read multiple graphs from Neo4j instances,
  * query across multiple graphs, and construct new graphs on top of multiple
  * existing graphs.
  */
object MultipleGraphLab extends App {
  // Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // Toy data for loading into Neo4j harness
  val socialNetworkFixture =
    s"""|CREATE (a:Person { name: 'Alice', age: 10 })
        |CREATE (b:Person { name: 'Bob', age: 20})
        |CREATE (c:Person { name: 'Carol', age: 15})
        |CREATE (a)-[:FRIEND_OF { since: '23/01/1987' }]->(b)
        |CREATE (b)-[:FRIEND_OF { since: '12/12/2009' }]->(c)""".stripMargin

  // Start a Neo4j instance and populate it with social network data
  val neo4j = Neo4jHelpers.startNeo4jHarness(dataFixture = socialNetworkFixture)

  // Register Graph Data Sources (GDS)
  session.registerSource(
    Namespace("socialNetwork"),
    CommunityNeo4jGraphDataSource(neo4j.dataSourceConfig))

  // Access the graph via its qualified graph name
  val socialNetwork = session.graph("socialNetwork.graph")

  socialNetwork.cypher("MATCH (n) RETURN n").getRecords.show

  // Lab challenge:
  //  - Install Desktop, use Browser to set up
  //  - Load multiple graphs from Neo4j instances
  //  - Use the new Cypher features to play around with querying across
  //    multiple graphs and creating new graphs that connect them

  // Shutdown Neo4j test instance
  neo4j.close()
}
