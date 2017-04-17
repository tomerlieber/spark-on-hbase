package x.spark.hbase

import java.util

import org.apache.hadoop.hbase.filter.{Filter, FuzzyRowFilter}
import org.apache.hadoop.hbase.util.Pair

/**
  * Created by Tomer.
  */
object HBaseUtils {

  // transform columns into an array of (column family, qualifier)
  def toColumnFamilyAndQualifier(columns: Seq[String]) = {
    columns.map(s => {
      val splitArray = s.split(":")
      val columnFamily = splitArray(0)
      val qualifier = splitArray(1)
      (columnFamily, qualifier)
    }).toArray
  }

  /**
    * Scan with fuzzy
    * Fuzzy Row Filter is server-side filter which preforms fast-forwarding based on the fuzzy row key mask provided by user.
    * The filter takes as parameters row key and a mask info.
    * For example we want to find all people with the same name and the row key format is PersonID-PersonName (where
    * each ID has fixed length of 8 chars), the fuzzy row key we are looking for is "?????????-Tomer". To tell
    * which positions are fixed and which are not fixed, the second parameter is byte array (mask info) with zeros
    * whose values are "fixed" and ones at the "non-fixed" positions.
    * so the mask info we are looking for is new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1)
    */
  def createFuzzyFilter(rowKeys: Seq[String], nonFixLetter: Option[Char]) : Filter = {

    // transform a row key into a pair of:
    // 1. bytes array of the row key
    // 2. 'mask info' which is represented by bytes array where 0 is "fixed" letter and 1 is "non-fixed" letter.
    //    we decide which letter is fixed according to the input "non-fixed" letter.
    val transformedRowKeys: Seq[Pair[Array[Byte], Array[Byte]]] = rowKeys.map(rowKey => {
      new Pair(rowKey.getBytes, rowKey.map(c => {
        nonFixLetter match {
          case Some(l) => if (c == l) 1.toByte else 0.toByte
          case None => 0.toByte
        }
      }).toArray)
    })

    // create list of fuzzy keys. each element is a pair of:
    // 1. row key bytes array
    // 2. mask info
    val fuzzyKeys = new util.ArrayList[Pair[Array[Byte], Array[Byte]]]()
    transformedRowKeys.foreach(fuzzyKey => fuzzyKeys.add(fuzzyKey))

    // Set fuzzy filter
    new FuzzyRowFilter(fuzzyKeys)
  }
}
