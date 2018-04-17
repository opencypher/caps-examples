package org.opencypher.example.lab1

import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{Node, Relationship, RelationshipType}

/**
  * Demonstrates basic usage of the CAPS API by loading an example network via Scala case classes and running a Cypher
  * query on it.
  */
object CaseClassExample extends App {

  // 1) Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // 2) Load social network data via case class instances
  val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

  // 3) Query graph with Cypher
  val results = socialNetwork.cypher(
    """|MATCH (a:Person)-[r:FRIEND_OF]->(b)
       |RETURN a.name, b.name, r.since""".stripMargin
  )

  // 4) Convert to maps and print to console
  println(results.getRecords.collect.mkString("\n"))
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
