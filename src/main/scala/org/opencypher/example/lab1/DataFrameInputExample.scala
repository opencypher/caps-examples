package org.opencypher.example.lab1

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}

import scala.collection.JavaConverters._

/**
  * Demonstrates basic usage of the CAPS API by loading an example network from existing [[DataFrame]]s and
  * running a Cypher query on it.
  */
object DataFrameInputExample extends App {
  // 1) Create CAPS session and retrieve Spark session
  implicit val session: CAPSSession = CAPSSession.local()
  val spark = session.sparkSession

  // 2) Generate some DataFrames that we'd like to interpret as a property graph.
  val nodesDF = SocialNetworkDataFrames.nodes(spark)
  val relsDF = SocialNetworkDataFrames.rels(spark)

  // 3) Generate node- and relationship scans that wrap the DataFrames and describe their contained data
  val personNodeMapping = NodeMapping
    .withSourceIdKey("id")
    .withImpliedLabel("Person")
    .withPropertyKey("name")
    .withPropertyKey("age")

  val friendOfMapping = RelationshipMapping
    .withSourceIdKey("id")
    .withSourceStartNodeKey("source")
    .withSourceEndNodeKey("target")
    .withRelType("FRIEND_OF")
    .withPropertyKey("since")

  val personTable = CAPSNodeTable(personNodeMapping, nodesDF)
  val friendsTable = CAPSRelationshipTable(friendOfMapping, relsDF)

  // 4) Create property graph from graph scans
  val graph = session.readFrom(personTable, friendsTable)

  // 5) Execute Cypher query and print results
  val result = graph.cypher("MATCH (n:Person) RETURN n.name")

  // 6) Collect results into string by selecting a specific column.
  //    This operation may be very expensive as it materializes results locally.
  // 6a) type safe version, discards values with wrong type
  val safeNames: Set[String] = result.getRecords.collect.flatMap(_ ("n.name").as[String]).toSet
  // 6b) unsafe version, throws an exception when value cannot be cast
  val unsafeNames: Set[String] = result.getRecords.collect.map(_ ("n.name").cast[String]).toSet

  println(safeNames)
}

object SocialNetworkDataFrames {
  def nodes(session: SparkSession): DataFrame = {
    val nodes = List(
      Row(0L, "Alice", 42L),
      Row(1L, "Bob", 23L),
      Row(2L, "Eve", 84L)
    ).asJava
    val nodeSchema = StructType(List(
      StructField("id", LongType, false),
      StructField("name", StringType, false),
      StructField("age", LongType, false))
    )
    session.createDataFrame(nodes, nodeSchema)
  }

  def rels(session: SparkSession): DataFrame = {
    val rels = List(
      Row(0L, 0L, 1L, "23/01/1987"),
      Row(1L, 1L, 2L, "12/12/2009")
    ).asJava
    val relSchema = StructType(List(
      StructField("id", LongType, false),
      StructField("source", LongType, false),
      StructField("target", LongType, false),
      StructField("since", StringType, false))
    )
    session.createDataFrame(rels, relSchema)
  }
}
