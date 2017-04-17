package x.spark.hbase

/**
  * Created by Tomer.
  */
object FilterType extends Enumeration {
  type FilterType = Value
  val RowStartAndStop, Fuzzy, GeneralFilter = Value
}
