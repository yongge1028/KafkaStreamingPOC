package OriginalWorking

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {
  @transient private var instance: SQLContext = null

  // Instantiate SQLContext on demand
  def getInstance(sparkContext: SparkContext): SQLContext = synchronized {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
