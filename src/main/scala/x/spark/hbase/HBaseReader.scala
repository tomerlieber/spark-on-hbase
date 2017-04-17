package x.spark.hbase

import org.apache.hadoop.hbase.filter._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

//import org.apache.spark.rdd.RDD
//import scala.reflect.runtime.universe.TypeTag
//import scala.reflect.ClassTag

/**
  * Created by Tomer.
  */

trait HBaseReader extends HBaseConfig {

  def select(columns: String*): HBaseReader

  def from(tableName: String): HBaseReader

  def where(filter: Filter): HBaseReader

  def whereRowKeysPrefixes(rowKeys: Seq[String]): HBaseReader

  def withRowStart(startRow: String): HBaseReader

  def withRowStop(rowStop: String): HBaseReader

  def setCaching(caching: Int): HBaseReader

  def setBatchSize(batchSize: Int): HBaseReader

  def load()(implicit sc: SparkContext): RDD[Product] // support generic -> : RDD[S] [S <: Product : ClassTag : TypeTag]
}