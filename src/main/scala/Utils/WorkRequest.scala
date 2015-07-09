package Utils

import java.text.SimpleDateFormat
import java.util.Calendar

import com.google.common.net.InetAddresses

/**
 * Created by faganpe on 28/06/2015.
 */
class WorkRequest extends Serializable {
  private var count: Int = 0
  private var partition: Int = 0

  /* Start of the random generation values used to influence the data that is produced */
  //  val r = scala.util.Random

  def this(count: Int, partition: Int) {
    this()
    this.count = count
    this.partition = partition
  }

  def getCount: Int = {
    return count
  }

  def getPartition: Int = {
    return partition
  }

  def getIPGenRand(randNum: Int): String = {
    //    val r = scala.util.Random
    if (randNum % 2 == 0) getIPRand()
    else getIPAddressSkew("132.146.5")
    //      getIPAddressSkew("132.146.5")
  }

  /* End of the random generation values used to influence the data that is produced */

  def getIPAddressSkew(IPSubnet: String): String = {
    val r = scala.util.Random
    val dotCount = IPSubnet.count(_ == '.') // let's count the number of dots
    if (dotCount == 3) IPSubnet // return the complete IP address without making anything up
    else if (dotCount == 2) IPSubnet + "." + r.nextInt(255)
    else if (dotCount == 1) IPSubnet + "." + r.nextInt(255) + "." + r.nextInt(255)
    else IPSubnet // otherwise just return the original ip string
  }

  def getIPRand(): String = {
    val r = scala.util.Random
    InetAddresses.fromInteger(r.nextInt()).getHostAddress()
  }

  def randNum(ranNum: Int): Int = {
    val r = scala.util.Random
    r.nextInt(ranNum)
  }

  override def equals(o: Any): Boolean =
  {
    if (this == o) return true
    if (o == null || getClass != o.getClass) return false
    val that: WorkRequest = o.asInstanceOf[WorkRequest]
    if (count != that.count) return false
    if (partition != that.partition) return false
    return true
  }

  override def hashCode: Int =
  {
    return partition
  }

  override def toString: String =
  {
    var retVal: String = null
    val r = scala.util.Random
    var s = ""
    for (a <- 1 to getCount) {
      val currentTimeForDirPart = Calendar.getInstance().getTime()

      // these dates need to be declared here so that the spark worker's work correctly and conform to the values
      // outside of the map
      val formatDate = new SimpleDateFormat("YYYY-MM-dd HH:MM:ss.SSSSSS")
      val formatDateDuration = new SimpleDateFormat("ss.SSSSSS")
      val formatDateDay = new SimpleDateFormat("YYYY-MM-dd")
      val formatDateHour = new SimpleDateFormat("HH")

      // start of define hours and mins and maybe secs here
      val formatDateDayForDir = new SimpleDateFormat("YYYY-MM-dd")
      val formatDateHourForDir = new SimpleDateFormat("HH")
      val formatDateMinuteForDir = new SimpleDateFormat("mm")
      val formatDateSecondForDir = new SimpleDateFormat("ss")
      val formatDateMilliSecondForDir = new SimpleDateFormat("SSS")
      val flowDay = formatDateDayForDir.format(currentTimeForDirPart)
      val flowHour = formatDateHourForDir.format(currentTimeForDirPart)
      val flowMinute = formatDateMinuteForDir.format(currentTimeForDirPart)
      val flowSecond = formatDateSecondForDir.format(currentTimeForDirPart)
      val flowMilliSecond = formatDateMilliSecondForDir.format(currentTimeForDirPart)
      // end of define hours and mins and maybe secs here

      // get the current time for flowDuration so we get variability
      val currentTime = Calendar.getInstance().getTime()

      val flowTimestamp = formatDate.format(currentTimeForDirPart)
      val flowDuration = formatDateDuration.format(currentTime)

      val SourceIPString = getIPGenRand(r.nextInt())
      val DestIPString = InetAddresses.fromInteger(r.nextInt()).getHostAddress()

      // start of maps
      val protoMap = Map(0 -> "udp", 1 -> "tcp", 2 -> "icmp", 3 -> "tcp", 4 -> "tcp")
      val flowDirMap = Map(0 -> "->", 1 -> "<?>", 2 -> "<->", 3 -> "?>", 4 -> "->", 5 -> "->")
      val flowStatMap = Map(0 -> "FSPA_FSPA", 1 -> "CON", 2 -> "INT", 3 -> "FA_FA",
        4 -> "SPA_SPA", 5 -> "S_", 6 -> "URP", 7 -> "CON", 8 -> "CON", 9 -> "CON", 10 -> "CON")
      //  val ipGenMap = Map(0 -> getIPAddressSkew("132.146.5"), 1 -> getIPRand())
      val sTosMap = Map(0 -> 0, 1 -> 3, 2 -> 2, 3 -> 2, 4 -> 2)
      val dTosMap = Map(0 -> 0, 1 -> 3, 2 -> 2, 3 -> 4)
      val totPktsMap = Map(0 -> randNum(2350), 1 -> randNum(128)) // big and small
      val totBytesMap = Map(0 -> randNum(128)) // big and small
      val labelMap = Map(0 -> "flow=From-Botnet-V44-ICMP",
          1 -> "flow=Backgrund-TCP-Attempt",
          2 -> "flow=From-Normal-V44-CVUT-WebServer",
          3 -> "flow=Background-google-analytics14",
          4 -> "flow=Background-UDP-NTP-Established-1",
          5 -> "flow=From-Botnet-V44-TCP-CC107-IRC-Not-Encrypted",
          6 -> "flow=Background-google-analytics4",
          7 -> "flow=Background-google-analytics9",
          8 -> "flow=From-Normal-V44-UDP-CVUT-DNS-Server")
      // end of maps

      s ++= flowTimestamp + "," + flowDuration + "," + protoMap(r.nextInt(5)) + "," +
        SourceIPString + "," + flowDirMap(r.nextInt(6)) + "," + DestIPString + "," +
        r.nextInt(65535) + "," + flowStatMap(r.nextInt(11)) + "," + sTosMap(r.nextInt(3)) +
        "," + dTosMap(r.nextInt(4)) + "," + totPktsMap(r.nextInt(2)) + "," +
        totBytesMap(r.nextInt(1)) + "," + labelMap(r.nextInt(9)) + "\n"

      //      s ++= flowTimestamp + "," + flowDuration + "\n"
    }
    return s
//    return return partition + "," + count;
  }

}
