package OriginalWorking

/** Case class for converting RDD to DataFrame */
// Define the schema using a case class.
// Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
// you can use custom classes that implement the Product interface.
case class Record(starttime: String)
