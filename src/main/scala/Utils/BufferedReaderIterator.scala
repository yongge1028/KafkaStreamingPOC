package Utils

/**
  * Created by 801762473 on 16/11/2015.
  */

import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream
import java.io.FileInputStream

class BufferedReaderIterator(reader: BufferedReader) extends Iterator[String] {
  override def hasNext() = reader.ready
  override def next() = reader.readLine()
}

object GzFileIterator {
  def apply(file: java.io.File, encoding: String) = {
    new BufferedReaderIterator(
      new BufferedReader(
        new InputStreamReader(
          new GZIPInputStream(
            new FileInputStream(file)), encoding)))
  }
}
