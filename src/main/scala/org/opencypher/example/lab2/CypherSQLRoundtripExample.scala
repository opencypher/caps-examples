package org.opencypher.example.lab2

import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.file.FileCsvGraphDataSource
import org.opencypher.spark.impl.CAPSConverters._

/**
  * Demonstrates usage patterns where Cypher and SQL can be interleaved in the
  * same processing chain, by using the tabular output of a Cypher query as a
  * SQL table, and using the output of a SQL query as an input driving table
  * for a Cypher query.
  */
object CypherSQLRoundtripExample extends App {
  // 1) Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // 2) Register a file based data source at the session
  //    It contains a purchase network graph called 'products'
  val graphDir = getClass.getResource("/csv").getFile
  session.registerSource(Namespace("myDataSource"), FileCsvGraphDataSource(rootPath = graphDir))

  // 3) Load social network data via case class instances
  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

  // 4) Query for a view of the people in the social network
  val result = socialNetwork.cypher(
    """|MATCH (p:Person)
       |RETURN p.age AS age, p.name AS name
    """.stripMargin
  )

  // 5) Register the result as a table called people
  result.getRecords.asCaps.toDF().createOrReplaceTempView("people")

  // 6) Query the registered table using SQL
  val sqlResults = session.sql("SELECT age, name FROM people")

  // 7) Use the results from the SQL query as driving table for a Cypher query on a graph contained in the data source
  val result2 = session.graph("myDataSource.products").cypher(
    s"""
       |MATCH (c:Customer {name: name})-->(p:Product)
       |RETURN c.name, age, p.title
     """.stripMargin, drivingTable = Some(sqlResults))

  // 8) Print the results
  result2.show
}
