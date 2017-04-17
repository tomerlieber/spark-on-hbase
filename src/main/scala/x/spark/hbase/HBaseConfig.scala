package x.spark.hbase

/**
  * Created by Tomer.
  */
trait HBaseConfig {

  def addConfiguration(config: Map[String, String])
}