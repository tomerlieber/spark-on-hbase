package x.spark.hbase

import org.apache.spark.SparkContext

/**
  * Created by Tomer.
  */
trait HBaseDelete extends HBaseConfig {

  def from(tableName: String): HBaseDelete

  def delete()(implicit sc: SparkContext): Unit
}