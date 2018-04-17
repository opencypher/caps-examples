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

import org.apache.spark.graphx.{Edge, Graph}
import org.opencypher.okapi.api.io.conversion.NodeMapping
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.CAPSSession._
import org.opencypher.spark.api.io.CAPSNodeTable
import org.opencypher.spark.api.io.EntityTable.SparkTable

/**
  * Round trip CAPS -> GraphX -> CAPS
  *
  * This example demonstrates how CAPS results can be used to construct a GraphX graph and invoke a GraphX algorithm
  * on it. The computed ranks are imported back into CAPS and used in a Cypher query.
  */
object GraphXPageRankExample extends App {

  // 1) Create CAPS session
  implicit val session = CAPSSession.local()

  // 2) Load social network data via case class instances
  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

  // 3) Query graph with Cypher
  val nodes = socialNetwork.cypher(
    """|MATCH (n:Person)
       |RETURN id(n), n.name""".stripMargin)

  val rels = socialNetwork.cypher(
    """|MATCH (:Person)-[r]->(:Person)
       |RETURN startNode(r), endNode(r)
    """.stripMargin
  )

  // 4) Create GraphX compatible RDDs from nodes and relationships
  val graphXNodeRDD = nodes.getRecords.asDataFrame.rdd.map(row => row.getLong(0) -> row.getString(1))
  val graphXRelRDD = rels.getRecords.asDataFrame.rdd.map(row => Edge(row.getLong(0), row.getLong(1), ()))

  // 5) Compute Page Rank via GraphX
  val graph = Graph(graphXNodeRDD, graphXRelRDD)
  val ranks = graph.pageRank(0.0001).vertices //.join(graphXNodeRDD).map { case (_, (rank, name)) => name -> rank }

  // 6) Convert RDD to DataFrame
  val rankTable: SparkTable = session.sparkSession.createDataFrame(ranks)
    .withColumnRenamed("_1", "id")
    .withColumnRenamed("_2", "rank")

  // 7) Create property graph from rank data
  val ranksNodeMapping = NodeMapping.on("id").withPropertyKey("rank")
  val rankNodes = session.readFrom(CAPSNodeTable(ranksNodeMapping, rankTable))

  // 8) Mount both graphs in the session
  session.store("ranks", rankNodes)
  session.store("sn", socialNetwork)

  rankNodes.nodes("r").show
  socialNetwork.nodes("s").show

  // 9) Query across both graphs to print names with corresponding ranks, sorted by rank
  val result = session.cypher(
    """|FROM GRAPH ranks
       |MATCH (r)
       |WITH id(r) as id, r.rank as rank
       |FROM GRAPH sn
       |MATCH (p:Person)
       |WHERE id(p) = id
       |RETURN p.name as name, rank
       |ORDER BY rank DESC""".stripMargin)

  result.getRecords.show
  //+---------------------------------------------+
  //| name                 | rank                 |
  //+---------------------------------------------+
  //| 'Carol'              | 1.4232365145228216   |
  //| 'Bob'                | 1.0235131396957122   |
  //| 'Alice'              | 0.5532503457814661   |
  //+---------------------------------------------+

}
