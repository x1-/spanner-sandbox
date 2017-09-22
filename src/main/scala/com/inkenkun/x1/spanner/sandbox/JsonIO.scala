package com.inkenkun.x1.spanner.sandbox

import java.time.{Instant, ZoneId, ZonedDateTime}

import com.google.cloud.spanner._

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.Exception._

object JsonIO {

  val fiveMinutes: Long = 60 * 5

  /**
    * データを作成します。
    * @param args
    */
  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      println(usage)
      sys.exit(1)
    }
    val start      = System.currentTimeMillis()
    val command    = args(0)
    val instanceId = args(1)
    val databaseId = args(2)

    val spannerOptions   : SpannerOptions      = SpannerOptions.newBuilder.build
    val spanner          : Spanner             = spannerOptions.getService

    val projectId        : String              = spannerOptions.getProjectId
    val db               : DatabaseId          = DatabaseId.of(projectId, instanceId, databaseId)
    val dbClient         : DatabaseClient      = spanner.getDatabaseClient(db)

    allCatch withApply {
      t => t.printStackTrace()
    } andFinally {
      spanner.close()
      println("closed client")
    } apply {
      command match {
        case "query" =>
          if (args.length < 4) {
            println(usageQuery)
            sys.exit(1)
          }
          val table = args(3)
          query(dbClient, statement(table), start)
        case "write" =>
          if (args.length < 5) {
            println(usageWrite)
            sys.exit(1)
          }
          val table = args(3)
          val num   = args(4).toInt
          write(dbClient, table, num)
        case _ =>
          println(s"$command is not implemented yet.")
      }
    }
    println(s"time: ${System.currentTimeMillis() - start} millis")
    sys.exit(0)
  }

  private val usage =
    """
      |Usage:
      |  sbt runMain "com.inkenkun.x1.spanner.sandbox.JsonIO <command> <instance_id> <database_id>"
      |
      |  command:
      |    write: write data without transaction
      |  instance_id:
      |    Cloud Spanner instance id (string)
      |  database_id:
      |    Cloud Spanner database id (string)
    """.stripMargin

  private val usageWrite =
    """
      |Usage:
      |  sbt runMain "com.inkenkun.x1.spanner.sandbox.JsonIO write <instance_id> <database_id> <table_name> <num>"
      |
      |  command:
      |    writeWithoutTx: write data without transaction
      |  instance_id:
      |    Cloud Spanner instance id (string)
      |  database_id:
      |    Cloud Spanner database id (string)
      |  table_name:
      |    Cloud Spanner table name (string)
      |  num:
      |    the amount of data generated
    """.stripMargin

  private val usageQuery =
    """
      |Usage:
      |  sbt runMain "com.inkenkun.x1.spanner.sandbox.JsonIO query <instance_id> <database_id> <table_name>"
      |
      |  command:
      |    writeWithoutTx: write data without transaction
      |  instance_id:
      |    Cloud Spanner instance id (string)
      |  database_id:
      |    Cloud Spanner database id (string)
      |  table_name:
      |    Cloud Spanner table name (string)
    """.stripMargin

  private val tableJson =
    """
      |CREATE TABLE json (
      |  unique     STRING(MAX) NOT NULL,
      |  sz         STRING(MAX) NOT NULL,
      |  ps         STRING(MAX) NOT NULL,
      |  rad        INT64 NOT NULL,
      |  measure    STRING(MAX) NOT NULL,
      |) PRIMARY KEY (unique, sz, ps, rad)
    """.stripMargin

  case class Measure (
    time  : String,
    num1  : Long,
    num2  : Long
  )
  case class Measures (
    unique: String,
    sz    : String,
    ps    : String,
    rad   : Long,
    items : Seq[Measure] = Seq.empty[Measure]
  )
  case class Measure2 (
    unique  : String,
    sz      : String,
    ps      : String,
    rad     : Long,
    measure : String
  )


  private def write(client: DatabaseClient, table: String, num: Int): Unit = {
    val today       = ZonedDateTime.now(ZoneId.of("UTC")).withSecond(0).withNano(0)
    val yesterday   = today.minusDays(1)
    val baseEpoch   = yesterday.toEpochSecond
    val chunks      = 2000

    val szs  = (1 to   1).map(x => s"s$x")
    val pss  = (1 to  10).map(x => s"p$x")
    val rads = (1 to  20).map(x => x.toLong)
    val uqs  = (1 to 100).map(x => randomString)

    val mutations = new ArrayBuffer[Mutation](chunks)
    var counter   = 0L
    println(s"${szs.size * pss.size * rads.size * uqs.size} data prepared")

    for (
      sz     <- szs;
      ps     <- pss;
      rad    <- rads;
      unique <- uqs
    ) {
      val measures = (1 to num).map(buildMeasure(baseEpoch))
      val mutation = buildMutation(table, sz, ps, rad, unique, measures.asJson.pretty(Printer.noSpaces.copy(dropNullKeys=false)))
      mutations += mutation
      counter   += 1
      if (counter % chunks == 0) {
        client.write(mutations.asJava)
        println(s"$counter mutations inserted.")
        mutations.clear()
      }
    }
    println(s"writing $counter number data done [example]")
  }

  private def buildMutation(table: String, sz: String, ps: String, rad: Long, unique: String, json: String) =
    Mutation.newInsertBuilder(table)
      .set("unique")
      .to(unique)
      .set("sz")
      .to(sz)
      .set("ps")
      .to(ps)
      .set("rad")
      .to(rad)
      .set("measure")
      .to(json)
      .build()

  private def buildMeasure(baseEpoch: Long)(n: Int): Measure = {
    val epoch    = baseEpoch + (fiveMinutes * n)
    val time     = BqDateTimeFormat.format(ZonedDateTime.ofInstant(Instant.ofEpochSecond(epoch), zone))
    val num1     = randomNumber(2000)
    val num2     = num1 / 10

    Measure (
      time   = n.toString,
      num1   = num1,
      num2   = num2
    )
  }

  val zone: ZoneId = ZoneId.of("UTC")
  val random: Random = new Random(new java.security.SecureRandom())
  private def randomString: String = random.alphanumeric.take(24).mkString
  private def randomNumber(n: Int): Int = random.nextInt(n)

  private def statement(table: String): Statement.Builder = Statement.newBuilder(
    s"""
      |SELECT
      |  unique,
      |  sz,
      |  ps,
      |  rad,
      |  measure
      |FROM
      |  $table
      |WHERE
      |  ps IN ('p1', 'p2', 'p3')
    """.stripMargin)

  private def query(client: DatabaseClient, statement: Statement.Builder, start: Long = 0): Unit = {
    val rs = client
      .singleUse()
      .executeQuery(
        statement
          .build()
      )
    println(s"executeQuery: ${System.currentTimeMillis() - start} millis")
    val measures = resultSetToMeasure2(rs)
    println(s"ResultSet.get: ${System.currentTimeMillis() - start} millis")
    println(s"got ${measures.size} data.")
  }
  private def resultSetToMeasure(rs: ResultSet): Seq[Measures] = Iterator.continually {
    if (rs.next()) {
      val json = rs.getString("measure")
      val biJson = decode[Seq[Measure]](json) match {
        case Right(xs) => xs
        case Left(e) => Seq.empty[Measure]
      }

      Measures (
        unique = rs.getString("unique"),
        sz     = rs.getString("sz"),
        ps     = rs.getString("ps"),
        rad    = rs.getLong("rad"),
        items  = biJson
      )

    } else null
  }.takeWhile(_ != null).toSeq

  private def resultSetToMeasure2(rs: ResultSet): Seq[Measure2] = Iterator.continually {
    if (rs.next()) {
      Measure2 (
        unique  = rs.getString("unique"),
        sz      = rs.getString("sz"),
        ps      = rs.getString("ps"),
        rad     = rs.getLong("rad"),
        measure = rs.getString("measure")
      )
    } else null
  }.takeWhile(_ != null).toSeq

}
