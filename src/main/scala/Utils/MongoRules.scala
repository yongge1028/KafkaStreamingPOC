package Utils

import com.mongodb.CursorType
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.{Imports, MongoDBList}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by faganpe on 26/12/2015.
  */
object MongoRules {

  // close the mongo connection
  sys addShutdownHook {
    println("About to close the mongo connection")
    mongoConn.close()
  }

  // get DB server connection
  val mongoConn = MongoConnection()

  // create a new DB and Collection if not present or use existing one
  val collection = mongoConn("rulelist")("rulelist")

  def retreiveSQLRules (): Array[AnyRef] = {
    // find and limit fields
    println("List only rulelist field:")
    val emptyCondition = MongoDBObject.empty
    val fields = MongoDBObject("email" -> 1)

    val rulesSQL = collection.find(emptyCondition, fields)

    for (x <- collection.find(emptyCondition, fields)) println(x.get("email"))

    var SQLArrayBuffer = ArrayBuffer[String]()

    for (x <- collection.find(emptyCondition, fields)) {
      SQLArrayBuffer += x.get("email").toString
      println(SQLArrayBuffer)
    }

    val SQLArray: Array[AnyRef] = SQLArrayBuffer.toArray

    SQLArray.foreach( sqlToRun => println("Running SQL Alert: " + sqlToRun))

    return SQLArray
  }

  // add 1st Document
//  val newObj = MongoDBObject("name" -> "alex", "email" -> "alex.seasonal@gmail.com", "number" -> "1234567890")
//  collection += newObj

  // all docs
  println("all docs:")
  collection.find foreach (println _)

  val allDocs = collection.find()

  def main(args: Array[String]): Unit = {
    retreiveSQLRules()
  }

}
