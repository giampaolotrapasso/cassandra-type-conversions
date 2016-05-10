package io.radicalbit.typeconversion

import com.datastax.driver.core.Cluster

import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq
import scala.util.{Success, Try}

object TypeConversions {


  def main(args: Array[String]): Unit = {

    val types = List(
      "ascii",
      "bigint",
      "blob",
      "boolean",
      "counter",
      "date",
      "decimal",
      "double",
      "float",
      "inet",
      "int",
      "smallint",
      "text",
      "time",
      "timestamp",
      "timeuuid",
      "tinyint",
      "uuid",
      "varchar",
      "varint")

    val typeProduct = for {
      t1 <- types
      t2 <- types if t1 != t2
    } yield (t1, t2)

    val t = Range(0, 1000).zip(typeProduct)
    val keySpace = "conversions"

    val statements = for {
      (index, (oldType, newType)) <- t
      cql1 = s"CREATE TABLE ${keySpace}.table${index} (pk int, myfield ${oldType}, PRIMARY KEY (pk))"
      cql2 = s"ALTER TABLE ${keySpace}.table${index} ALTER myfield TYPE ${newType};"
    } yield (cql1, cql2, oldType, newType)

    val clusterBuilder = Cluster.builder.addContactPoint("localhost")
    val cluster = clusterBuilder.build()
    val session = cluster.newSession()

    Try(session.execute(s" DROP KEYSPACE ${keySpace}"))

    session.execute(s" CREATE KEYSPACE IF NOT EXISTS ${keySpace} WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")

    val allowed: List[Option[(String, String)]] = statements.toList.map { s =>
      //println(s"execute ${s._1}")
      session.execute(s._1)
      //println(s"execute ${s._2}")
      val alterTry = Try(session.execute(s._2))
      alterTry match {
        case Success(_) => Some(s._4, s._3)
        case _ => None
      }
    }

    val filtered = allowed.flatten

    val original: Map[String, List[(String, String)]] = filtered.groupBy(s => s._1)
    val modified = original map { case (k, v) => k -> v.map(v => v._2) }

    modified.foreach {
      case (k, v) =>
        println(v + " to " + k)
    }

  }
}