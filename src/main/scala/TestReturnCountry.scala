/**
 * Created by faganp on 2/26/15.
 */
object TestReturnCountry extends App {

  def setupMaxMindDB(): Unit = {
    println("Setup DB complete")
  }

  def lookupCountry(Country: String): String = {
    return "UK"
  }
}
