package x.spark.hbase

import org.apache.spark.SparkContext

/**
  * Created by Tomer.
  */
trait HBaseWriter extends HBaseConfig {

  def into(tableName: String): HBaseWriter

  def toColumns(columns: String*): HBaseWriter

  def save()(implicit sc: SparkContext): Unit
}