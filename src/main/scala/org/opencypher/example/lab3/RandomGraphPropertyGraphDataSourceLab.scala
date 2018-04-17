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
package org.opencypher.example.lab3

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.opencypher.okapi.api.graph.{GraphName, Namespace, PropertyGraph}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.spark.api.CAPSSession

/**
  * Generator for random graphs based on node connection probability
  *
  * The node dataframe consists of three columns.
  *   -id: contains a unique long identifier
  *   -longProperty: a long values
  *   -stringProperty: a string value
  *
  *  The relationship dataframe consists of four columns:
  *   -id: a unique long identifier
  *   -src: id of the start node
  *   -tgt: id of the end node
  *   -probability: a float values used to determine whether source and start node are connected
  *
  * @param connectionProbability threshold probability used to check if two nodes are connected
  * @param caps CAPS session
  */
class RandomGraphGenerator(val connectionProbability: Double)(implicit val caps: CAPSSession) {

  val spark: SparkSession = caps.sparkSession
  import spark.implicits._

  require(connectionProbability >= 0, "Connection probability must be higher than or equal to zero")
  require(connectionProbability <= 1, "Connection probability must be lower than or equal to one")

  /**
    * Generates a random graph, represented by two tuples. One for the nodes and one for the relationships. The size
    * parameter determines the number of nodes present in the generated graph.
    *
    * @param size number of nodes present in the generated graph
    * @return node and relationship DataFrames
    */
  def generate(size: Long): (DataFrame, DataFrame) = {
    val inputDf = spark.range(0, size, 1)

    val nodes = inputDf
      .withColumn("longProperty", $"id")
      .withColumn("stringProperty", $"id".cast(StringType))

    val sourceNodes = nodes
      .select("id")
      .withColumnRenamed("id", "src")

    val targetNodes = nodes
      .select("id")
      .withColumnRenamed("id", "tgt")

    val rels = sourceNodes.crossJoin(targetNodes)
      .withColumn("id", functions.monotonically_increasing_id())
      .withColumn("probability", functions.rand())
      .filter($"probability" <= functions.lit(connectionProbability))

    nodes -> rels
  }

}

/**
  * CAPS property graph datas source that generates random graphs based on the probability that two nodes are connected.
  * The size of the random graph can be controlled using the graph name, e.g. 'size100' will output a graph with 100
  * nodes.
  *
  * @param generator an instance of the random graph generator
  * @param caps CAPS session
  */
case class RandomPropertyGraphDataSource(generator: RandomGraphGenerator)(implicit val caps: CAPSSession) extends PropertyGraphDataSource {
  /**
    * Returns `true` if the data source stores a graph under the given [[org.opencypher.okapi.api.graph.GraphName]].
    *
    * @param name name of the graph within the data source
    * @return `true`, iff the graph is stored within the data source
    */
  override def hasGraph(name: GraphName): Boolean = ???

  /**
    * Returns the [[org.opencypher.okapi.api.graph.PropertyGraph]] that is stored under the given name.
    *
    * @param name name of the graph within the data source
    * @return property graph
    */
  override def graph(name: GraphName): PropertyGraph = ???

  /**
    * Returns the [[org.opencypher.okapi.api.schema.Schema]] of the graph that is stored under the given name.
    *
    * This method gives implementers the ability to efficiently retrieve a graph schema from the data source directly.
    * For reasons of performance, it is highly recommended to make a schema available through this call. If an efficient
    * retrieval is not possible, the call is typically forwarded to the graph using the [[org.opencypher.okapi.api.graph.PropertyGraph#schema]] call, which may require materialising the full graph.
    *
    * @param name name of the graph within the data source
    * @return graph schema
    */
  override def schema(name: GraphName): Option[Schema] = ???

  /**
    * Stores the given [[org.opencypher.okapi.api.graph.PropertyGraph]] under the given [[org.opencypher.okapi.api.graph.GraphName]] within the data source.
    *
    * @param name name under which the graph shall be stored
    * @param graph property graph
    */
  override def store(
    name: GraphName,
    graph: PropertyGraph
  ): Unit = throw NotImplementedException("Store is not implemented for Random Graph Data Source")

  /**
    * Deletes the [[org.opencypher.okapi.api.graph.PropertyGraph]] within the data source that is stored under the given [[org.opencypher.okapi.api.graph.GraphName]].
    *
    * @param name name under which the graph is stored
    */
  override def delete(name: GraphName): Unit = throw NotImplementedException("Delete is not implemented for Random Graph Data Source")

  /**
    * Returns the [[org.opencypher.okapi.api.graph.GraphName]]s of all [[org.opencypher.okapi.api.graph.PropertyGraph]]s stored within the data source.
    *
    * @return names of stored graphs
    */
  override def graphNames: Set[GraphName] = ???
}

object TestApp extends App {

  implicit val caps: CAPSSession = CAPSSession.local()

  val generator = new RandomGraphGenerator(0.5)
  val dataSource = RandomPropertyGraphDataSource(generator)

  caps.registerSource(Namespace("random"), dataSource)

  caps.cypher(
    """
      |FROM GRAPH random.size10
      |MATCH (n)-[r]->(m)
      |RETURN n, r.probability, m
    """.stripMargin).getRecords.show
}
