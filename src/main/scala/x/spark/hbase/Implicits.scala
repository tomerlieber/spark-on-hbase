package x.spark.hbase

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Tomer.
  */
object Implicits {

  implicit class HBaseReaderExtension(sc: SparkContext) {
    def toHBaseReader: HBaseReader = new HBaseHandler(null, null)
  }

  implicit class HBaseWriterExtension[T <: Product](rdd: RDD[T]) {
    def toHBaseWriter: HBaseWriter = new HBaseHandler(null, rdd.map(_.asInstanceOf[Product]))
  }

  implicit class HBaseDeleteExtension(rdd: RDD[String]) {
    def toHBaseDelete: HBaseDelete = new HBaseHandler(rdd, null)
  }
}