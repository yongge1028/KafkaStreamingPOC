/**
 * Created by faganp on 2/26/15.
 */
object TestMaxMindSingleton extends App {

//  val mm = MaxMindSingleton.getInstance()

  for (a <-1 to 100000) {
//    println(mm.getCountry("132.146.5.1"))
//    mm.getCountry("132.146.5.1")
    MaxMindSingleton.getInstance().getCountry("132.146.5.1")
  }

}
