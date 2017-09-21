package com.inkenkun.x1.spanner.sandbox

import java.nio.file.{Path, Paths, Files, StandardOpenOption}
import java.nio.charset.StandardCharsets
import java.time.{Instant, ZoneId, ZonedDateTime}

import com.google.cloud.Timestamp
import com.google.cloud.spanner._
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.Exception._

object Inserter {
  
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
    val dbAdminClient    : DatabaseAdminClient = spanner.getDatabaseAdminClient

    allCatch withApply {
      t => t.printStackTrace()
    } andFinally {
      spanner.close()
      println("closed client")
    } apply {
      command match {
        case "create" =>
          createDatabase(dbAdminClient, db)
        case "writeWithoutTx" =>
          if (args.length < 5) {
            println(usageWriteWithoutTx)
            sys.exit(1)
          }
          val table = args(3)
          val num   = args(4).toInt
          writeWithoutTx(dbClient, table, num)
        case _ =>
          val zd = ZonedDateTime.of(2017, 9, 1, 0, 0, 0, 0, ZoneId.of("Asia/Tokyo"))
          val is = Instant.ofEpochSecond(zd.toEpochSecond)
          val zd2 = ZonedDateTime.ofInstant(is, ZoneId.of("Asia/Tokyo"))
          val zd3 = ZonedDateTime.of(2017, 9, 1, 0, 5, 0, 0, ZoneId.of("Asia/Tokyo"))

          val today     = ZonedDateTime.now(ZoneId.of("UTC")).withSecond(0).withNano(0)
          val yesterday = today.minusDays(1)

          println(s"zd.toEpochSecond: ${zd.toEpochSecond}")
          println(s"zd3.toEpochSecond: ${zd3.toEpochSecond}")
          println(s"zd2: ${zd2.toString}")
          println(s"${zd3.minusDays(1).toString}")
          println(s"${ZonedDateTime.now(ZoneId.of("UTC")).toString}")
          println(s"today: ${today.toString}, yesterday: ${yesterday.toString}")
          println(s"$command is not implemented yet.")
      }
    }
    println(s"time: ${System.currentTimeMillis() - start} millis")
    sys.exit(0)
  }

  private val usage =
    """
      |Usage:
      |  sbt runMain "com.inkenkun.x1.spanner.sandbox.Inserter <command> <instance_id> <database_id>"
      |
      |  command:
      |    create: create database
      |    writeWithoutTx: write data without transaction
      |    writeWithTx: write data with readwrite transaction
      |  instance_id:
      |    Cloud Spanner instance id (string)
      |  database_id:
      |    Cloud Spanner database id (string)
    """.stripMargin

  private val usageWriteWithoutTx =
    """
      |Usage:
      |  sbt runMain "com.inkenkun.x1.spanner.sandbox.Inserter writeWithoutTx <instance_id> <database_id> <table_name> <num>"
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

  private val tableExample =
    """
      |CREATE TABLE example (
      |  unique     STRING(MAX) NOT NULL,
      |  time       TIMESTAMP NOT NULL,
      |  time         STRING(MAX) NOT NULL,
      |  ps         STRING(MAX) NOT NULL,
      |  rad        INT64 NOT NULL,
      |  num1       INT64 NOT NULL,
      |  num2       INT64 NOT NULL
      |) PRIMARY KEY (unique, time, sz, ps, rad)
    """.stripMargin

  private def createDatabase(dbAdminClient: DatabaseAdminClient, id: DatabaseId): Unit = {
    val op: Operation[Database, CreateDatabaseMetadata] =
      dbAdminClient
        .createDatabase(
          id.getInstanceId.getInstance,
          id.getDatabase,
          List(tableExample).asJava
        )
    val db = op.waitFor().getResult
    println(s"Created database [${db.getId}]")
  }

  private def writeWithoutTx(client: DatabaseClient, table: String, num: Int): Unit = {
    val filePath    = Paths.get(s"${table}_${num}.json")
    val today       = ZonedDateTime.now(ZoneId.of("UTC")).withSecond(0).withNano(0)
    val yesterday   = today.minusDays(1)
    val baseEpoch   = yesterday.toEpochSecond
    val fiveMinutes = 60 * 5
    val chunks      = 2000

    val szs  = (1 to  10).map(x => s"s$x")
    val pss  = (1 to  10).map(x => s"p$x")
    val rads = (1 to 100).map(x => x.toLong)
    val uqs  = (1 to 100).map(x => randomString)

    val mutations = new ArrayBuffer[Mutation](chunks)
    val jsons     = new ArrayBuffer[String](chunks)
    var counter   = 0L
    println(s"${num * 5 * 10 * 100 * 100} data pwrepared")

    for (
      n      <- 1 to num;
      sz     <- szs;
      ps     <- pss;
      rad    <- rads;
      unique <- uqs
    ) {
      val epoch    = baseEpoch + (fiveMinutes * n)
      val num1     = randomNumber(2000)
      val num2     = num1 / 10
      val mutation = buildMutation(table, sz, ps, rad, unique, epoch, num1, num2)
      val example  = buildExample(sz, ps, rad, unique, epoch, num1, num2)
      mutations += mutation
      jsons     += example.toJson
      counter   += 1
      if (counter % chunks == 0) {
        client.write(mutations.asJava)
        write(filePath, jsons)
        println(s"$counter mutations inserted.")
        mutations.clear()
        jsons.clear()
      }
    }
//    val splited = split(mutations, 2000, Seq.empty[Seq[Mutation]])
//    for (i <- splited.indices) {
//      client.write(splited(i).asJava)
//      if (i % 100 == 0) println(s"${i * 2000} data inserted.")
//    }
    println(s"writing $counter number data done [example]")
  }

  private def buildMutation(table: String, sz: String, ps: String, rad: Long, unique: String, epoch: Long, num1: Int, num2: Int) =
    Mutation.newInsertBuilder(table)
      .set("unique")
      .to(unique)
      .set("time")
      .to(Timestamp.ofTimeSecondsAndNanos(epoch, 0))
      .set("sz")
      .to(sz)
      .set("ps")
      .to(ps)
      .set("rad")
      .to(rad)
      .set("num1")
      .to(num1)
      .set("num2")
      .to(num2)
      .build()

  private def buildExample(sz: String, ps: String, rad: Long, unique: String, epoch: Long, num1: Int, num2: Int): Example =
    Example (
      unique = unique,
      time   = BqDateTimeFormat.format(ZonedDateTime.ofInstant(Instant.ofEpochSecond(epoch), ZoneId.of("UTC"))),
      sz     = sz,
      ps     = ps,
      rad    = rad,
      num1   = num1,
      num2   = num2
    )

  private def write(path: Path, lines: Seq[String]): Unit = {
    Files.write(path, lines.asJava, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)
  }
  private def split[A](xs: Seq[A], n: Int, acc: Seq[Seq[A]]): Seq[Seq[A]] =
    if (xs.isEmpty) acc
    else {
      val (xs1, xs2) = xs.splitAt(n)
      split(xs2, n, acc :+ xs1)
    }

  val zone: ZoneId = ZoneId.of("UTC")
  val random: Random = new Random(new java.security.SecureRandom())
  private def randomString: String = random.alphanumeric.take(24).mkString
  private def randomNumber(n: Int): Int = random.nextInt(n)
}
