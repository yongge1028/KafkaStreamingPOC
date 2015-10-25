/**
 * Created by 801762473 on 14/10/2015.
 */
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.{AvroInputFormat, AvroWrapper}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkContext, SparkConf}

object ClouderaDemo {

  val sparkConf = new SparkConf()
  val sc = new SparkContext(sparkConf)

  val warehouse = "hdfs://quickstart/user/hive/warehouse/"

  val order_items_path = warehouse + "order_items"
  val order_items = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](order_items_path)

  val products_path = warehouse + "products"
  val products = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](products_path)

  val orders = order_items.map { x => (
    x._1.datum.get("order_item_product_id"),
    (x._1.datum.get("order_item_order_id"), x._1.datum.get("order_item_quantity")))
  }.join(
    products.map { x => (
      x._1.datum.get("product_id"),
      (x._1.datum.get("product_name")))
    }
  ).map(x => (
    scala.Int.unbox(x._2._1._1), // order_id
    (
      scala.Int.unbox(x._2._1._2), // quantity
      x._2._2.toString // product_name
      )
    )).groupByKey()

  val cooccurrences = orders.map(order =>
    (
      order._1,
      order._2.toList.combinations(2).map(order_pair =>
        (
          if (order_pair(0)._2 < order_pair(1)._2) (order_pair(0)._2, order_pair(1)._2) else (order_pair(1)._2, order_pair(0)._2),
          order_pair(0)._1 * order_pair(1)._1
          )
      )
      )
  )
  val combos = cooccurrences.flatMap(x => x._2).reduceByKey((a, b) => a + b)
  val mostCommon = combos.map(x => (x._2, x._1)).sortByKey(false).take(10)

  println(mostCommon.deep.mkString("\n"))

  exit


}
