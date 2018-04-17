package org.opencypher.example.lab3

import org.opencypher.example.utils.Neo4jHelpers
import org.opencypher.example.utils.Neo4jHelpers._
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.neo4j.CommunityNeo4jGraphDataSource

object Neo4jDataSourceExample extends App {

  // Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  val neo4j = Neo4jHelpers.startNeo4j(personNetwork)

  // Register Graph Data Sources (GDS)
  session.registerSource(Namespace("socialNetwork"), CommunityNeo4jGraphDataSource(neo4j.dataSourceConfig))

  // Access the graph via its qualified graph name
  val socialNetwork = session.graph("socialNetwork.graph")

  socialNetwork.cypher("MATCH (n) RETURN count(n)").show

  def personNetwork =
    s"""|CREATE (a:Person { name: 'Alice', age: 10 })
        |CREATE (b:Person { name: 'Bob', age: 20})
        |CREATE (c:Person { name: 'Carol', age: 15})
        |CREATE (a)-[:FRIEND_OF { since: '23/01/1987' }]->(b)
        |CREATE (b)-[:FRIEND_OF { since: '12/12/2009' }]->(c)""".stripMargin
}
