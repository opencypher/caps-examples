package org.opencypher.example.lab4

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.neo4j.hdfs.parquet.HdfsParquetGraphSource
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession

object HdfsParquetExample extends App {

  implicit val caps: CAPSSession = CAPSSession.local()

  val hdfsConfig = new Configuration()
  implicit val hdfs: FileSystem = FileSystem.get(hdfsConfig)

  val hdfsPath = new Path("hdfs://xxx/xxx")

  val dataSource = new HdfsParquetGraphSource(hdfsPath)

  caps.registerSource(Namespace("hdfs"), dataSource)

  caps.cypher(
    """
      |FROM GRAPH hdfs.start_trek
      |MATCH (n)
      |RETURN n
    """.stripMargin)
}
