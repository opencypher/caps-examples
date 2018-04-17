package org.opencypher.example.lab1

import org.apache.spark.sql.{DataFrame, functions}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.CAPSSession._

/**
  * Shows how to access a Cypher query result as a [[DataFrame]].
  */
object DataFrameOutputExample extends App {

  // 1) Create CAPS session and retrieve Spark session
  implicit val session: CAPSSession = CAPSSession.local()

  // 2) Load social network data via case class instances
  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

  // 3) Query graph with Cypher
  val results = socialNetwork.cypher(
    """|MATCH (a:Person)-[r:FRIEND_OF]->(b)
       |RETURN a.name, b.name, r.since""".stripMargin)

  // 4) Extract DataFrame representing the query result
  val df: DataFrame = results.getRecords.asDataFrame

  // 5) Select specific return items from the query result
  val projection: DataFrame = df.select(columnFor("a.name"), columnFor("b.name"))

  projection.show()
}

/**
  * Alternative to accessing a Cypher query result as a [[DataFrame]].
  */
object DataFrameOutputUsingAliasExample extends App {
  // 1) Create CAPS session and retrieve Spark session
  implicit val session: CAPSSession = CAPSSession.local()

  // 2) Load social network data via case class instances
  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

  // 3) Query graph with Cypher
  val results = socialNetwork.cypher(
    """|MATCH (a:Person)-[r:FRIEND_OF]->(b)
       |RETURN a.name AS person1, b.name AS person2, r.since AS friendsSince""".stripMargin)

  // 4) Extract DataFrame representing the query result
  val df: DataFrame = results.getRecords.asDataFrame

  // 5) Select aliased return items from the query result
  val projection: DataFrame = df
    .select("person1", "friendsSince", "person2")
    .orderBy(functions.to_date(df.col("friendsSince"), "dd/mm/yyyy"))

  projection.show()
}
