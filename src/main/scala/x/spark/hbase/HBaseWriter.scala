package x.spark.hbase

import org.apache.spark.SparkContext
//import scala.reflect.runtime.universe.TypeTag

/**
  * Created by Tomer.
  */
trait HBaseWriter extends HBaseConfig { // For generic Add [T <: Product] in class declartion

  def into(tableName: String): HBaseWriter

  def toColumns(columns: String*): HBaseWriter

  def save()(implicit sc: SparkContext): Unit // For generic add the parameter tag: TypeTag
}