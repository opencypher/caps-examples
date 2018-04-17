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

import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{Node, Relationship, RelationshipType}
import org.opencypher.spark.api.io.file.FileCsvGraphDataSource

/**
  * Demonstrates multiple graph capabilities by loading a social network from case class objects and a purchase network
  * from CSV data and schema files. The example connects both networks via matching user and customer names. A Cypher
  * query is then used to compute products that friends have bought.
  */
object MultipleGraphExample extends App {
  // Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // Load social network data via case class instances
  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)
  session.store("socialNetwork", socialNetwork)

  // Register a File-based data source in the Cypher session
  val csvFolder = getClass.getResource("/csv").getFile
  // Note: if files were stored in HDFS, change the data source to HdfsCsvPropertyGraphDataSource
  session.registerSource(Namespace("csv"), FileCsvGraphDataSource(rootPath = csvFolder))
  // access the graph via its qualified graph name
  val purchaseNetwork = session.graph("csv.products")

  // 5) Create new edges between users and customers with the same name
  val recommendationGraph = session.cypher(
    """|FROM GRAPH socialNetwork
       |MATCH (p:Person)
       |FROM GRAPH csv.products
       |MATCH (c:Customer)
       |WHERE p.name = c.name
       |CONSTRUCT ON socialNetwork, csv.products
       |  NEW (p)-[:IS]->(c)
       |RETURN GRAPH
    """.stripMargin
  ).getGraph

  // Query for product recommendations
  val recommendations = recommendationGraph.cypher(
    """|MATCH (person:Person)-[:FRIEND_OF]-(friend:Person),
       |      (friend)-[:IS]->(customer:Customer),
       |      (customer)-[:BOUGHT]->(product:Product)
       |RETURN DISTINCT product.title AS recommendation, person.name AS for
    """.stripMargin)

  recommendations.show
}

/**
  * Specify schema and data with case classes.
  */
object SocialNetworkData {

  case class Person(id: Long, name: String, age: Int) extends Node

  @RelationshipType("FRIEND_OF")
  case class Friend(id: Long, source: Long, target: Long, since: String) extends Relationship

  val alice = Person(0, "Alice", 10)
  val bob = Person(1, "Bob", 20)
  val carol = Person(2, "Carol", 15)

  val persons = List(alice, bob, carol)
  val friendships = List(Friend(0, alice.id, bob.id, "23/01/1987"), Friend(1, bob.id, carol.id, "12/12/2009"))
}
