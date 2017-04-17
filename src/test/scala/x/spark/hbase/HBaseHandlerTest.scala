package x.spark.hbase

import x.spark.hbase.Implicits._
import org.apache.commons.lang.time.StopWatch
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

/**
  * Created by Tomer.
  */
class HBaseHandlerTest extends FunSuite {

  // Init basic objects in order to preform the tests.
  val hBaseHostUrl = "xx.xxx.xxx.xx:1234"
  val tableName = ""

  // Build a SparkConf object that contains information about my application
  val sparkConf = new SparkConf()
    .setAppName("Spark on HBase Test")
    .setMaster("local[*]")
    .set("spark.hbase.host", hBaseHostUrl)

  // Create a SparkContext object which tells Spark how to access a cluster
  implicit val sc = new SparkContext(sparkConf)


  test("delete specific record from HBase") {


    val rowKey = "123"
    sc.parallelize(Array(rowKey)).toHBaseDelete.from(tableName).delete()
  }

  test("write data to HBase") {

    val numbers = sc.parallelize(0 to 9).map(x => (x.toString, "Tomer " + x))
    numbers.toHBaseWriter.into(tableName).toColumns("personInformation:Name").save()
  }

  test("read data from HBase with fuzzy filter") {

    val ids = Seq("1", "2", "4")

    val sw = new StopWatch

    sw.start()

    val rdd = sc.toHBaseReader.select("personInformation:Name").from(tableName).whereRowKeysPrefixes(ids).setCaching(1000).setBatchSize(100).load()

    println("Records number: " + rdd.count)

    sw.stop()

    println("The time to read with fuzzy " + sw)
  }

  test("read data from HBase with filter based on a key") {

    val sw = new StopWatch

    sw.start()

    val rowKey = 11
    val rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes(rowKey)))

    val rdd = sc.toHBaseReader.select("personInformation:Name").from(tableName).where(rowFilter).load()

    println("Records number: " + rdd.count())

    sw.stop()

    println("The time to read with row filter: " + sw)
  }
}