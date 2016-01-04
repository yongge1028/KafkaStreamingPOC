package Utils

import com.mongodb.CursorType
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.ValidBSONType.ObjectId
import com.mongodb.casbah.commons.{Imports, MongoDBList}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by faganpe on 26/12/2015.
  */
object MongoOps {

  // close the mongo connection
  sys addShutdownHook {
    println("About to close the mongo connection")
    mongoConn.close()
  }

  // get DB server connection
  val mongoConn = MongoConnection()

  // connect to "mongodb02" host, port 42017
//  val mongoConn = MongoConnection("mongodb02", 42017)

  // create a new DB and Collection if not present or use existing one
  val collection = mongoConn("rulelist")("rulelist")

  def retreiveSQLRules (): Array[AnyRef] = {
    // find and limit fields
    println("List only rulelist field:")
    val emptyCondition = MongoDBObject.empty
    val fields = MongoDBObject("rulesql" -> 1)

    val rulesSQL = collection.find(emptyCondition, fields)

    for (x <- collection.find(emptyCondition, fields)) println(x.get("rulesql"))

    var SQLArrayBuffer = ArrayBuffer[String]()

    for (x <- collection.find(emptyCondition, fields)) {
      println("x is x: " + x)
      SQLArrayBuffer += x.get("rulesql").toString
      println(SQLArrayBuffer)
    }

    val SQLArray: Array[AnyRef] = SQLArrayBuffer.toArray

    SQLArray.foreach( sqlToRun => println("Running SQL Alert: " + sqlToRun))

    return SQLArray
  }

  def retreiveSQLRulesCursor (): MongoCursor = {
    // find and limit fields
    println("List only rulelist field:")
    val emptyCondition = MongoDBObject.empty
    val fields = MongoDBObject("name" -> 1, "rulesql" -> 1) // only select the rulesql field in the mongo collection
//    val fields = MongoDBObject("rulesql" -> 1) // only select the rulesql field in the mongo collection

    val rulesSQL = collection.find(emptyCondition, fields)
//    rulesSQL.foreach(println)

//    rulesSQL.foreach( x => println("Found something! %s".format(x("rulesql"))) )

    return rulesSQL
  }

  // Note: the $set mongo directive below just upserts the field value specified and does not update the whole document
  // like the mongo update call does
  def updateNumMessages(ruleName: String, mongoField: String, mongoValue: String): Unit = {
    val query = MongoDBObject("name" -> ruleName)
    val update = $set(mongoField -> mongoValue)
//    println("About to update mongoID: " + mongoID)
//    // WriteConcern.Acknowledged will slow mongo down a lot, but currently we don't need this to be really performant
    val result = collection.update( query, update, upsert=true, concern=WriteConcern.Acknowledged )
    println( "Number updated: " + result.getN )
    for ( c <- collection.find ) println( c )

  }

  // add 1st Document
//  val newObj = MongoDBObject("name" -> "alex", "rulesql" -> "alex.seasonal@gmail.com", "number" -> "1234567890")
//  collection += newObj

  // all docs
  println("all docs:")
  collection.find foreach (println _)

  val allDocs = collection.find()

  def main(args: Array[String]): Unit = {
//    retreiveSQLRules()
    retreiveSQLRulesCursor()
//    val findID: Option[ObjectId] = "56843900bf8e393455b900b1".getAs[ObjectId]("_id")
    updateNumMessages("r2", "nummessages", "415")
  }

}
