package org.opencypher.example.lab4

import com.sksamuel.exts.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.neo4j.harness.{ServerControls, TestServerBuilders}
import org.neo4j.hdfs.parquet.HdfsParquetGraphSource
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.neo4j.Neo4jConfig
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.impl.io.neo4j.Neo4jGraphLoader

object HadoopParquetTest extends App {

  implicit val configuration: Configuration = new Configuration()
  implicit val fs: FileSystem = FileSystem.get(configuration)

  // ----------------------------
  // Create a local spark session
  // ----------------------------
  val conf = new SparkConf(true)
  conf.set("spark.driver.allowMultipleContexts", "true")
  conf.set("spark.sql.caseSensitive", "false") // Option required for spark case insensitivity
  implicit val sparkSession: SparkSession = SparkSession
    .builder()
    .config(conf)
    .master("local[*]")
    .appName("caps-test")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("WARN")

  // -----------------------------------------------------------------------
  // Create a local CAPS session which implicitly includes the spark session
  // -----------------------------------------------------------------------
  implicit val caps: CAPSSession = CAPSSession.create()(sparkSession)

  // ------------------------------------------------------
  // NEO embedded Server with data loaded via Cypher CREATE
  // ------------------------------------------------------
  val neo4jServer: ServerControls = CypherCluster.neo4jServer

  // --------------------------------
  // Return NEO graph as a CAPS graph
  // --------------------------------
  val neo4jConfig: Neo4jConfig = {
    Neo4jConfig(
      uri = neo4jServer.boltURI(),
      user = "anonymous",
      password = Some("password"),
      encrypted = false)
  }
  val neo4jHost: String = {
    val scheme = neo4jServer.boltURI().getScheme
    val userInfo = s"${neo4jConfig.user}:${neo4jConfig.password.get}@"
    val host = neo4jServer.boltURI().getAuthority
    s"$scheme://$userInfo$host"
  }
  val capsGraph: CAPSGraph = Neo4jGraphLoader.fromNeo4j(neo4jConfig)

  // -------------------------
  // HDFS Parquet Graph Source
  // -------------------------
  val parquetRoot = new Path("/tmp/parquet")
  if (!fs.exists(parquetRoot)) fs.mkdirs(parquetRoot) else fs.delete(parquetRoot, true)
  private val hdfsParquetGraphSource = new HdfsParquetGraphSource(parquetRoot)

  // ------------------------------------------
  // Store the NEO CAPS graph into HDFS Parquet
  // ------------------------------------------
  val graphName = GraphName("com.foo.bar")
  hdfsParquetGraphSource.store(graphName, capsGraph)

  // ----------------------------------------------
  // Perform CYPHER query on the HDFS Parquet Graph
  // ----------------------------------------------
  val printOptions = PrintOptions(stream = Console.out, columnWidth = 100, margin = 2)
  hdfsParquetGraphSource
    .graph(graphName)
    .cypher("match(n) return n")
    .getRecords
    .show(printOptions)

  // --------------------------------------------
  // Now read one of the NODES as a raw DataFrame
  // --------------------------------------------
  sparkSession.read.parquet("/tmp/parquet/com/foo/bar/VIEW_SET/NODES_EDGES_BY_LABEL/nodes/Person").show(100)

  // --------------------------------------------
  // Now read one of the EDGES as a raw DataFrame
  // --------------------------------------------
  sparkSession.read.parquet("/tmp/parquet/com/foo/bar/VIEW_SET/NODES_EDGES_BY_LABEL/edges/SERVED_ON").show(100)

  System.exit(0)


  object CypherCluster extends Logging {

    val neo4jServer: ServerControls = TestServerBuilders.newInProcessBuilder()
      .withConfig("dbms.security.auth_enabled", "true")
      .withFixture(userFixture)
      .withFixture(dataFixture)
      .newServer()
    logger.info("Embedded NEO is UP")

    def userFixture: String = "CALL dbms.security.createUser('anonymous', 'password', false)"

    def dataFixture: String =
      """
        |CREATE (picard:Person:Officer   {name: 'Jean Luc Picard', rank: 'Captain'}),
        |       (riker:Person:Officer    {name: 'William Riker', rank: 'Commander'}),
        |       (worf:Person:Officer     {name: 'Worf son of Mogh', rank: 'Lieutenant Commander'}),
        |       (crusher:Person:Officer  {name: 'Beverley Crusher', rank: 'Commander'}),
        |       (data:Person:Officer     {name: 'Data', rank: 'Lieutenant Commander'}),
        |       (sela:Person:Officer     {name: 'Sela', rank: 'Subcommander'}),
        |       (obrien:Person           {name: 'Miles OBrien', rank: 'Petty Officer'})
        |
        |CREATE (enterprise:Starship      {name: 'USS Enterprise', designation: 'NCC-1701-D', commission_date: 2365, max_warp: 9.8}),
        |       (defiant:Starship         {name: 'USS Defiant', designation: 'NX-74205', commission_date: 2370, max_warp: 9.6}),
        |       (stargazer:Starship       {name: 'Stargazer', designation: 'NCC-2893', commission_date: 2333, max_warp: 8.9}),
        |       (pagh:Starship            {name: 'IKS Pagh', designation: 'PGH-D7', commission_date: 2363, max_warp: 9.2})
        |
        |CREATE (federation:Affiliation {name: 'Federation'}),
        |       (rse:Affiliation {name: 'Romulan Star Empire'}),
        |       (ke:Affiliation {name: 'Klingon Empire'})
        |
        |CREATE (earth:Planet {name: 'Earth'}),
        |       (qonos:Planet {name: 'Qonos'}),
        |       (romulus:Planet {name: 'Romulus'})
        |
        |CREATE (galaxy_class:Class {class: 'Galaxy Class'}),
        |       (defiant_class:Class {class: 'Defiant Class'}),
        |       (constellation_class:Class {class: 'Constellation Class'}),
        |       (d7_class:Class {class: 'D7-Class'})
        |
        |CREATE (human:Race {name: 'Human', peaceful: false}),
        |       (klingon:Race {name: 'Klingon', peaceful: true}),
        |       (android:Race {name: 'Android', peaceful: true}),
        |       (romulan:Race {name: 'Romulan', peaceful: false}),
        |       (vulcan:Race {name: 'Vulcan', peaceful: true})
        |
        |CREATE (picard)-[:SERVED_ON {role: 'Captain'}]->(enterprise),
        |       (picard)-[:SERVED_ON {role: 'Bridge Officer'}]->(stargazer),
        |       (riker)-[:SERVED_ON {role: 'First Officer'}]->(enterprise),
        |       (riker)-[:SERVED_ON {role: 'First Officer'}]->(enterprise),
        |       (obrien)-[:SERVED_ON {role: 'Transporter Chief'}]->(enterprise),
        |       (worf)-[:SERVED_ON {role: 'Tactical'}]->(enterprise),
        |       (worf)-[:SERVED_ON {role: 'First Officer'}]->(defiant),
        |       (crusher)-[:SERVED_ON {role: 'Chief Medical Officer'}]->(enterprise),
        |       (data)-[:SERVED_ON {role: 'Science Officer'}]->(enterprise)
        |
        |CREATE (picard)-[:AFFILIATED_TO]->(federation),
        |       (riker)-[:AFFILIATED_TO]->(federation),
        |       (worf)-[:AFFILIATED_TO]->(federation),
        |       (worf)-[:AFFILIATED_TO]->(ke),
        |       (crusher)-[:AFFILIATED_TO]->(federation),
        |       (data)-[:AFFILIATED_TO]->(federation),
        |       (sela)-[:AFFILIATED_TO]->(rse)
        |
        |CREATE (human)-[:MEMBERS_OF]->(federation),
        |       (vulcan)-[:MEMBERS_OF]->(federation),
        |       (klingon)-[:MEMBERS_OF]->(ke),
        |       (romulan)-[:MEMBERS_OF]->(rse)
        |
        |CREATE (picard)-[:IS_A]->(human),
        |       (riker)-[:IS_A]->(human),
        |       (worf)-[:IS_A]->(klingon),
        |       (crusher)-[:IS_A]->(human),
        |       (data)-[:IS_A]->(android),
        |       (sela)-[:IS_A]->(romulan),
        |       (obrien)-[:IS_A]->(human)
        |
        |CREATE (enterprise)-[:BUILT_BY]->(federation),
        |       (defiant)-[:BUILT_BY]->(federation),
        |       (stargazer)-[:BUILT_BY]->(federation),
        |       (pagh)-[:BUILT_BY]->(ke)
        |
        |CREATE (federation)-[:HOMEWORLD]->(earth),
        |       (ke)-[:HOMEWORLD]->(qonos),
        |       (rse)-[:HOMEWORLD]->(romulus)
        |
        |CREATE (federation)-[:ALLIED_TO]->(ke),
        |       (ke)-[:ALLIED_TO]->(federation),
        |       (ke)-[:AT_WAR]->(rse),
        |       (rse)-[:AT_WAR]->(ke),
        |       (federation)-[:NON_AGRESSION_PACT]->(rse),
        |       (rse)-[:NON_AGRESSION_PACT]->(federation)
        |
        |CREATE (enterprise)-[:TYPE_OF {flag_ship: true}]->(galaxy_class),
        |       (defiant)-[:TYPE_OF {flag_ship: false}]->(defiant_class),
        |       (stargazer)-[:TYPE_OF {flag_ship: false}]->(constellation_class),
        |       (pagh)-[:TYPE_OF {flag_ship: false}]->(d7_class)
      """.stripMargin
  }
}

