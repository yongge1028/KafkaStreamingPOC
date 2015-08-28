package Utils

import com.github.tototoshi.csv._
import java.io._

import scala.io.Source

/**
 * Created by 801762473 on 27/08/2015.
 */
object PopulateRandomString {

  def returnRand() : List[List[String]] = {

    val rVal = scala.util.Random
    val reader = CSVReader.open(new File("src/main/resources/randommaps.csv"))
    val readerAll = reader.all()
    readerAll // returns a List

//    println("32,2015-04-30 18:20:43,none,12345678910,147.149.7.125,54944,74.125.0.65,30486,17,4,592,133792,True,True,True,True,True,True,100,none,Charlie Gas Site1,Charlie Gas,Energy,CNI,1,UK,United Kingdom,null,null,51.5,-0.13000488,BT-BT,NULL,14298,United States,CA,Mountain View,37.419205,-122.0574,Google-Google,NULL,59,null,null,Charlie Gas Site1,Charlie Gas,Energy,CNI,1,UK,,,,,NULL,,,,,,,,,,,,2015,4,30,18,20")
//    return "32,2015-04-30 18:20:43,none,12345678910,147.149.7.125,54944,74.125.0.65,30486,17,4,592,133792,True,True,True,True,True,True,100,none,Charlie Gas Site1,Charlie Gas,Energy,CNI,1,UK,United Kingdom,null,null,51.5,-0.13000488,BT-BT,NULL,14298,United States,CA,Mountain View,37.419205,-122.0574,Google-Google,NULL,59,null,null,Charlie Gas Site1,Charlie Gas,Energy,CNI,1,UK,,,,,NULL,,,,,,,,,,,,2015,4,30,18,20"

  }

}
