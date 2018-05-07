package org.neo4j.morpheus.examples

import org.apache.hadoop.fs.Path
import org.neo4j.hdfs.parquet.HdfsParquetGraphSource
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession

object TheConstruct extends App {

  implicit val session: CAPSSession = CAPSSession.local()

  session.registerSource(Namespace("myLocal"), HdfsParquetGraphSource(new Path("file:///my/local/file/path")))

  session.registerSource(Namespace("datalake"), HdfsParquetGraphSource(new Path("hdfs:///my/remote/hdfs/path")))

  session.cypher("CREATE GRAPH myLocal.graph1 { CONSTRUCT NEW (:A {foo: 1}) RETURN GRAPH }")
}