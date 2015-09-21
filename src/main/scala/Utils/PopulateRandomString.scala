package Utils

import com.github.tototoshi.csv._
import java.io._

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * Created by 801762473 on 27/08/2015.
 */
object PopulateRandomString extends App {

//  def returnRand() : List[List[String]] = {
  def returnRand() : ArrayBuffer[String] = {

  val rVal = scala.util.Random

  val  urls = urlses(getClass.getClassLoader)
  println(urls.filterNot(_.toString.contains("ivy")).mkString("\n"))

//  val src = Source.fromFile("src/main/resources/randommaps.csv").getLines
  // get the csv from the classpath rather than a source dir
  val src = Source.fromInputStream(getClass.getResourceAsStream("/randommaps1.csv")).getLines

  // assuming first line is a header
//  val numLines = Source.fromFile("src/main/resources/randommaps.csv").getLines.size
  val headerLine = src.take(1).next
  val numCols = headerLine.split(",").map(_.trim).length

  println("Number of columns in csv is : " + numCols)
  println("Number of lines in csv is : " + numLines)

  // declare an array ready to add the lines below
//  var lineArr:Array[String] = new Array[String](numLines)
  var lineArr = ArrayBuffer[String]()

  println("lineArr length is : " + lineArr.length)

  // print file contents, because we have taken from the file with the take(1) above we have
  // effectivly stripped off the file header
  for(l <- src) {
    println(l)
    lineArr += l
  }

//  println("lineArr length is : " + lineArr.length)
//
//  for(x <- lineArr) {
//    println("Line in array is : " + x)
//  }
//
//  println("Array length is : " + lineArr.length)

  return lineArr

  // processing remaining lines
//  for(l <- src) {
//    // split line by comma and process them
//    l.split(",").map { c =>
//      println(c)
//      // your logic here
//    }
//  }

//    val readerAll = reader.all()
//    reader.close()
//    println(readerAll(0)) // returns a List

//    println("32,2015-04-30 18:20:43,none,12345678910,147.149.7.125,54944,74.125.0.65,30486,17,4,592,133792,True,True,True,True,True,True,100,none,Charlie Gas Site1,Charlie Gas,Energy,CNI,1,UK,United Kingdom,null,null,51.5,-0.13000488,BT-BT,NULL,14298,United States,CA,Mountain View,37.419205,-122.0574,Google-Google,NULL,59,null,null,Charlie Gas Site1,Charlie Gas,Energy,CNI,1,UK,,,,,NULL,,,,,,,,,,,,2015,4,30,18,20")
//    return "32,2015-04-30 18:20:43,none,12345678910,147.149.7.125,54944,74.125.0.65,30486,17,4,592,133792,True,True,True,True,True,True,100,none,Charlie Gas Site1,Charlie Gas,Energy,CNI,1,UK,United Kingdom,null,null,51.5,-0.13000488,BT-BT,NULL,14298,United States,CA,Mountain View,37.419205,-122.0574,Google-Google,NULL,59,null,null,Charlie Gas Site1,Charlie Gas,Energy,CNI,1,UK,,,,,NULL,,,,,,,,,,,,2015,4,30,18,20"

  }

  def headerLine: String = {
    val src = Source.fromInputStream(getClass.getResourceAsStream("/randommaps.csv")).getLines
    val headerLine = src.take(1).next
    headerLine // return headerLine
  }

  def numLines(): Int = {
    val numLines = Source.fromFile("src/main/resources/randommaps.csv").getLines.size
    return numLines
  }

  def urlses(cl: ClassLoader): Array[java.net.URL] = cl match {
    case null => Array()
    case u: java.net.URLClassLoader => u.getURLs() ++ urlses(cl.getParent)
    case _ => urlses(cl.getParent)
  }

  returnRand()

}
