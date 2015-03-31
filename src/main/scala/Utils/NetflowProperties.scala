package Utils

/**
 * Created by faganpe on 31/03/15.
 */

import SimpleLib.SimpleLibContext
import com.typesafe.config._

object NetflowProperties {

  def main(args: Array[String]): Unit = {

    // example of how system properties override; note this
    // must be set before the config lib is used
    System.setProperty("netflow-lib.whatever", "This value comes from a system property")

    // Load our own config values from the default location, application.conf
    val conf = ConfigFactory.load()
    println("The application name  is: " + conf.getString("netflow-app.name"))

    // In this simple app, we're allowing SimpleLibContext() to
    // use the default config in application.conf ; this is exactly
    // the same as passing in ConfigFactory.load() here, so we could
    // also write "new SimpleLibContext(conf)" and it would be the same.
    // (simple-lib is a library in this same examples/ directory).
    // The point is that SimpleLibContext defaults to ConfigFactory.load()
    // but also allows us to pass in our own Config.
    val netflowContext = new SimpleLibContext()
    netflowContext.printSetting("netflow-lib.foo")
//    context.printSetting("netflow-lib.foo")
//    context.printSetting("netflow-lib.hello")
//    context.printSetting("netflow-lib.whatever")

  }

}
