package io.radicalbit.typeconversion

import com.datastax.driver.core.{SocketOptions, Session, Cluster}

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
      //"counter",
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


    val inputs = for {
      (index, (oldType, newType)) <- Range(0, 1000).zip(typeProduct)
    } yield ConversionInput(index, oldType, newType)



    val keySpace = "conversions"

    val clusterBuilder = Cluster.builder.addContactPoint("localhost")
      .withSocketOptions(
        new SocketOptions()
          .setConnectTimeoutMillis(5000))

    val cluster = clusterBuilder.build()
    val session = cluster.newSession()

    Try(session.execute(s" DROP KEYSPACE ${keySpace}"))

    session.execute(s" CREATE KEYSPACE IF NOT EXISTS ${keySpace} WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")

    val statements1 = simpleFieldStatement(inputs.toList, keySpace)
    val result1 = tryConversions(statements1, keySpace, session)

    val statements2 = partitionKeyStatement(inputs.toList, keySpace)
    val result2 = tryConversions(statements2, keySpace, session)

    val statements3 = clusteringColumn(inputs.toList, keySpace)
    val result3 = tryConversions(statements3, keySpace, session)

    println("Simple field")
    printList(result1)
    println("Partition key")
    printList(result2)
    println("Clustering column")
    printList(result3)

    session.close()
    cluster.close()


  }

  def printList(list: Map[String, List[String]]) = {
    list.foreach {
      case (k, v) =>
        println(v + " to " + k)
    }
  }

  def tryConversions(statements: List[StatementParameter], keySpace: String, session: Session) = {


    val allowed: List[Option[(String, String)]] = statements.map { s =>
      //println(s"execute ${s.create}")
      session.execute(s.create)
      //println(s"execute ${s.alter}")
      val alterTry = Try(session.execute(s.alter))
      alterTry match {
        case Success(_) => Some(s.newField, s.oldField)
        case _ => None
      }
    }

    val filtered = allowed.flatten

    val original: Map[String, List[(String, String)]] = filtered.groupBy(s => s._1)
    original map { case (k, v) => k -> v.map(v => v._2) }

  }

  def simpleFieldStatement(inputs: List[ConversionInput], keySpace: String): List[StatementParameter] = {
    val statements = for {
      input <- inputs
      cql1 = s"CREATE TABLE ${keySpace}.sftable${input.index} (pk int, myfield ${input.oldField}, PRIMARY KEY (pk));"
      cql2 = s"ALTER TABLE ${keySpace}.sftable${input.index} ALTER myfield TYPE ${input.newField};"
    } yield StatementParameter(cql1, cql2, input.oldField, input.newField)
    statements
  }

  def partitionKeyStatement(inputs: List[ConversionInput], keySpace: String): List[StatementParameter] = {
    val statements = for {
      input <- inputs
      cql1 = s"CREATE TABLE ${keySpace}.pktable${input.index} (pk ${input.oldField}, afield int, PRIMARY KEY (pk));"
      cql2 = s"ALTER TABLE ${keySpace}.pktable${input.index} ALTER pk TYPE ${input.newField};"
    } yield StatementParameter(cql1, cql2, input.oldField, input.newField)
    statements
  }

  def clusteringColumn(inputs: List[ConversionInput], keySpace: String): List[StatementParameter] = {
    val statements = for {
      input <- inputs
      cql1 = s"CREATE TABLE ${keySpace}.cctable${input.index} (pk int, cc ${input.oldField}, afield int, PRIMARY KEY (pk, cc));"
      cql2 = s"ALTER TABLE ${keySpace}.cctable${input.index} ALTER cc TYPE ${input.newField};"
    } yield StatementParameter(cql1, cql2, input.oldField, input.newField)
    statements
  }

}