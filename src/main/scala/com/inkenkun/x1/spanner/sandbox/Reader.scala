package com.inkenkun.x1.spanner.sandbox

import com.google.cloud.spanner._
import scala.util.control.Exception._

object Reader {

  /**
    * データを読み取ります。
    * @param args
    */
  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      println(usage)
      sys.exit(1)
    }
    val start      = System.currentTimeMillis()
    val command    = args(0)
    val instanceId = args(1)
    val databaseId = args(2)
    val table      = args(3)

    val spannerOptions   : SpannerOptions      = SpannerOptions.newBuilder.build
    val spanner          : Spanner             = spannerOptions.getService

    val projectId        : String              = spannerOptions.getProjectId
    val db               : DatabaseId          = DatabaseId.of(projectId, instanceId, databaseId)
    val dbClient         : DatabaseClient      = spanner.getDatabaseClient(db)
    val dbAdminClient    : DatabaseAdminClient = spanner.getDatabaseAdminClient

    allCatch withApply {
      t => t.printStackTrace()
    } andFinally {
      spanner.close()
      println("closed client")
    } apply {
      command match {
        case "query" =>
          if (args.length < 5) {
            println(usageQuery)
            sys.exit(1)
          }
          val timestamp = args(4)
          query(dbClient, statement(table), timestamp, start)
        case "queryWithIndex" =>
          if (args.length < 5) {
            println(usageQuery)
            sys.exit(1)
          }
          val timestamp = args(4)
          query(dbClient, statementWithIndex(table), timestamp, start)
        case "million" =>
          queryWithoutTimestamp(dbClient, statementMillion(table), start)
        case "read" =>
        case _ =>
          println(usage)
      }
    }
    println(s"time: ${System.currentTimeMillis() - start} millis")
    sys.exit(0)
  }

  private val usage =
    """
      |Usage:
      |  sbt runMain "com.inkenkun.x1.spanner.sandbox.Reader <command> <instance_id> <database_id> <table>"
      |
      |  command:
      |    query: publish query
      |    read: read data with index
      |  instance_id:
      |    Cloud Spanner instance id (string)
      |  database_id:
      |    Cloud Spanner database id (string)
      |  table:
      |    Cloud Spanner table name (string)
    """.stripMargin

  private val usageQuery =
    """
      |Usage:
      |  sbt runMain "com.inkenkun.x1.spanner.sandbox.Reader query|queryWithIndex <instance_id> <database_id> <table> <time>"
      |
      |  command:
      |    query: publish query
      |    read: read data with index
      |  instance_id:
      |    Cloud Spanner instance id (string)
      |  database_id:
      |    Cloud Spanner database id (string)
      |  table:
      |    Cloud Spanner table name (string)
      |  time:
      |    TIMESTAMP string ex) 2017-09-20 00:01:00
    """.stripMargin

  private def statement(table: String): Statement.Builder = Statement.newBuilder(
    s"""
      |SELECT
      |  unique,
      |  sz,
      |  ps,
      |  rad,
      |  sum(num1) as n1,
      |  sum(num2) as n2
      |FROM
      |  $table
      |WHERE
      |  time > TIMESTAMP(@time, "UTC")
      |  AND sz = 's1'
      |  AND ps = 'p1'
      |  AND rad = 1
      |GROUP BY
      |  unique,
      |  sz,
      |  ps,
      |  rad
    """.stripMargin)

  private def statementWithIndex(table: String): Statement.Builder = Statement.newBuilder(
    s"""
      |SELECT
      |  unique,
      |  sz,
      |  ps,
      |  rad,
      |  sum(num1) as n1,
      |  sum(num2) as n2
      |FROM
      |  ${table}@{force_index=idx_time_of_${table}}
      |WHERE
      |  time > TIMESTAMP(@time, "UTC")
      |GROUP BY
      |  unique,
      |  sz,
      |  ps,
      |  rad
    """.stripMargin)

  private def statementMillion(table: String): Statement.Builder = Statement.newBuilder(
    s"""
      |SELECT
      |  unique,
      |  sz,
      |  ps,
      |  rad,
      |  num1 as n1,
      |  num2 as n2
      |FROM
      |  ${table}
    """.stripMargin)

  private def query(client: DatabaseClient, statement: Statement.Builder, timestamp: String, start: Long = 0): Unit = {
    val rs = client
      .singleUse()
      .executeQuery(
        statement
          .bind("time").to(timestamp)
          .build()
      )
    println(s"executeQuery: ${System.currentTimeMillis() - start} millis")
    val examples = resultSetToExample(rs)
    println(s"ResultSet.get: ${System.currentTimeMillis() - start} millis")
    println(s"got ${examples.size} data.")
  }

  private def queryWithoutTimestamp(client: DatabaseClient, statement: Statement.Builder, start: Long = 0): Unit = {
    val rs = client
      .singleUse()
      .executeQuery(
        statement
          .build()
      )
    println(s"executeQuery: ${System.currentTimeMillis() - start} millis")
    val examples = resultSetToExample(rs)
    println(s"ResultSet.get: ${System.currentTimeMillis() - start} millis")
    println(s"got ${examples.size} data.")
  }

  private def resultSetToExample(rs: ResultSet): Seq[Example] = Iterator.continually {
    if (rs.next()) {
      Example (
        unique = rs.getString("unique"),
        time   = "",
        sz     = rs.getString("sz"),
        ps     = rs.getString("ps"),
        rad    = rs.getLong("rad"),
        num1   = rs.getLong("n1"),
        num2   = rs.getLong("n2")
      )
    } else null
  }.takeWhile(_ != null).toSeq

}
