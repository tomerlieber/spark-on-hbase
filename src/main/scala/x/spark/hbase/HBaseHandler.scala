package x.spark.hbase

import java.io.IOException
import java.util

import x.spark.hbase.FilterType.FilterType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{Filter, FuzzyRowFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes, Pair}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._

/**
  * Created by Tomer.
  */

// Added transient annotation is needed to prevent spark from serializing and sharing the data members across the nodes
protected case class HBaseHandler(rowKeysToDelete: RDD[String],
                                  rddToSave: RDD[Product],
                                  filterType: FilterType = null,
                                  @transient scan: Scan = new Scan(),
                                  @transient hBaseConf: Configuration = HBaseConfiguration.create(),
                                  @transient filter: Filter = null,
                                  columns: Array[(String, String)] = Array.empty) extends Serializable with HBaseDelete with HBaseWriter with HBaseReader {

  private val utf8Encoding = "UTF-8"
  private val hBaseHostKey = "spark.hbase.host"

  /////////////////
  // HBase Delete//
  /////////////////

  @throws(classOf[IllegalArgumentException])
  def delete()(implicit sc: SparkContext) = {

    val tableName = hBaseConf.get(TableInputFormat.INPUT_TABLE)
    require(Option(tableName).nonEmpty && tableName.nonEmpty, "Table name must be specified")
    require(Option(sc).nonEmpty, "Spark Context must be implicit specified")

    // Gets HBase url from the spark configuration
    val hBaseHostUrl = sc.getConf.get(hBaseHostKey)

    require(Option(hBaseHostUrl).nonEmpty && hBaseHostUrl.nonEmpty, "HBase url must be specified")

    // On each node open a connection to HBase and delete only the row keys in his memory
    rowKeysToDelete.foreachPartition(rowKeysPartition => {

      // Check if there any row keys to delete
      if (rowKeysPartition.nonEmpty) {

        // Configure HBase host for each partition
        val innerHBaseConf = HBaseConfiguration.create()
        innerHBaseConf.set(HConstants.ZOOKEEPER_QUORUM, hBaseHostUrl)

        // Create a HTable class to communicate with a single HBase table.
        val table = new HTable(innerHBaseConf, tableName)

        try {
          // Create list which each object specified record to delete
          val deletions = rowKeysPartition.map(rowKey => new Delete(rowKey.getBytes(utf8Encoding))).toList

          // TODO: think how to implement other kinds of deletes
          // delete all versions of al columns of the specified family
          // overrides previous calls to deleteColumn abd deleteColumns for the specified family
          // to delete specific families, execute deleteFamily for each family to delete
          //          new Delete("rowKey".getBytes(utf8Encoding)).deleteFamily("cf".getBytes(utf8Encoding))

          // delete all version of all column of the specified family
          // Overrides previous calls to delete column and deleteColumns for the specified family
          //          new Delete("rowKey".getBytes(utf8Encoding)).deleteColumns("cf".getBytes(utf8Encoding),"qu".getBytes(utf8Encoding))

          // Deletes the specified rows in bulk (because the delete function throw unexplained exception I use the batch function)
          table.batch(deletions.asJava) // TODO: maybe to use batch function with the result parameter
        }
        finally {

          // Releases any resources held or pending changes in internal buffers.
          table.close()
        }
      }
    })
  }

  /////////////////
  // HBase Writer//
  /////////////////

  @throws(classOf[IllegalArgumentException])
  def into(tableName: String) = {
    require(Option(tableName).nonEmpty && tableName.nonEmpty, "You must provide table name")
    require(Option(hBaseConf.get(TableOutputFormat.OUTPUT_TABLE)).isEmpty, "Table name has already been set")

    // Configure HBase for writing
    hBaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    this.copy()
  }

  @throws(classOf[IllegalArgumentException])
  def toColumns(columns: String*) = {
    require(Option(columns).nonEmpty && columns.nonEmpty && columns.head.nonEmpty, "You should provide at least one column")
    require(this.columns.isEmpty, "Columns have already been set")

    val transformedColumns = HBaseUtils.toColumnFamilyAndQualifier(columns)

    this.copy(columns = transformedColumns)
  }

  @throws(classOf[IllegalArgumentException])
  def save()(implicit sc: SparkContext) = {
    // support generic - add the parameter tag: TypeTag[T]

    val tableName = hBaseConf.get(TableOutputFormat.OUTPUT_TABLE)
    require(Option(tableName).nonEmpty && tableName.nonEmpty, "Table name must be specified")
    require(Option(columns).nonEmpty && columns.nonEmpty, "Columns must be specified")
    require(Option(sc).nonEmpty, "Spark Context must be implicit specified")

    // Gets HBase url from the spark configuration
    val hBaseHostUrl = sc.getConf.get(hBaseHostKey)

    require(Option(hBaseHostUrl).nonEmpty && hBaseHostUrl.nonEmpty, "HBase url must be specified")

    // Configure HBase host
    hBaseConf.set(HConstants.ZOOKEEPER_QUORUM, hBaseHostUrl)

    // Configure HBase job config for writing
    val jobConfig = Job.getInstance(hBaseConf)
    jobConfig.setOutputFormatClass(classOf[TableOutputFormat[String]])

    // Convert the product rdd to HBase put object in order to save to HBase
    rddToSave.map(convertToPut).saveAsNewAPIHadoopDataset(jobConfig.getConfiguration)

    // Convert a row of product object data to an HBase put object
    def convertToPut(product: Product): (ImmutableBytesWritable, Put) = {

      // Check if each column has exactly one value
      // For example: if we want to save to HBase product with 4 values (Tuple4), we need to receive 3 columns (because one is known which is the row key)
      require(product.productArity == columns.length + 1, "Each column must have exactly one value")

      // The first element in product (Tuple) is a row key
      val rowKey = product.productElement(0).toString
      val put = new Put(Bytes.toBytes(rowKey))

      // transform into stream of values that will be saved to HBase, drop the first value which is a row key
      val values = product.productIterator.toArray.map(_.toString).drop(1)

      // union each value with it's column family and qualifier
      val data = columns.zip(values).map { case (column, value) => (column._1, column._2, value) }

      // Add column family, qualifier and value to put object
      data.foreach { case (columnFamily, qualifier, value) =>
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value))
      }

      (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put)
    }
  }

  /////////////////
  // HBase Reader//
  /////////////////

  /**
    * Select which rows to scan. Column family and qualifier delimited by colon.
    * For example: select("personInformation : name", "personInformation : address")
    */
  @throws(classOf[IllegalArgumentException])
  def select(columns: String*): HBaseReader = {
    require(Option(columns).nonEmpty && columns.nonEmpty && columns.head.nonEmpty, "Invalid columns")
    require(this.columns.isEmpty, "Columns have already been set")

    // transform columns into an array of (column family, qualifier)
    val transformedColumns = HBaseUtils.toColumnFamilyAndQualifier(columns) // TODO: check if there any opposite operation to :_*

    // Set which columns to scan
    val newScan: Scan = transformedColumns.foldLeft(scan)((scan, column) => {
      val columnFamily = column._1
      val qualifier = column._2
      scan.addColumn(columnFamily.getBytes(utf8Encoding), qualifier.getBytes(utf8Encoding))
    })

    this.copy(scan = newScan, columns = transformedColumns)
  }

  @throws(classOf[IllegalArgumentException])
  def from(tableName: String) = {
    require(Option(tableName).nonEmpty && tableName.nonEmpty, "Invalid table name")
    require(Option(hBaseConf.get(TableInputFormat.INPUT_TABLE)).isEmpty, "Table name has already been set")

    // Configure HBase for reading and specifies the input table
    hBaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

    this.copy()
  }

  @throws(classOf[IllegalArgumentException])
  def where(filter: Filter) = {
    require(Option(filter).nonEmpty, "Filter can't be null")
    require(Option(this.filterType).isEmpty, "Filter has already been set")

    // Set a filter to the scan and specify you want to scan by general filter
    this.copy(scan = scan.setFilter(filter), filterType = FilterType.GeneralFilter)
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
  private def getFuzzyFilter(rowKeys: Seq[String], nonFixLetter: Option[Char]) = {

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

  /**
    * Scan the wanted row keys. Based on the fuzzy filter.
    * Put Attention: use this function only when you read from tables with the same length of row keys
    */
  @throws(classOf[IllegalArgumentException])
  def whereRowKeysPrefixes(rowKeys: Seq[String]) = {
    require(Option(rowKeys).isDefined, "Row keys mustn't be null")
    require(Option(filterType).isEmpty, "Filter has already been set")

    val fuzzyRowFilter = getFuzzyFilter(rowKeys, None)

    // set fuzzy filter and specify you want to scan by fuzzy filter
    this.copy(scan = scan.setFilter(fuzzyRowFilter), filterType = FilterType.Fuzzy)
  }

  /**
    * Scan start row
    */
  @throws(classOf[IllegalArgumentException])
  def withRowStart(startRow: String) = {
    require(Option(startRow).nonEmpty && startRow.nonEmpty, "Invalid start row")
    require(Option(this.filterType).isEmpty || this.filterType == FilterType.RowStartAndStop, "Filter has already been set")
    require(Option(scan.getStartRow).isEmpty, "Start row has already been set")

    // Set row start filter and specify you want to scan by row start
    this.copy(scan = scan.setStartRow(startRow.getBytes(utf8Encoding)), filterType = FilterType.RowStartAndStop)
  }

  /**
    * Scan stop row
    */
  @throws(classOf[IllegalArgumentException])
  def withRowStop(rowStop: String) = {
    require(Option(rowStop).nonEmpty && rowStop.nonEmpty, "Invalid stop row")
    require(Option(this.filterType).isEmpty || filterType == FilterType.RowStartAndStop, "Filter has already been set")
    require(scan.getStopRow == null, "Stop row has already been set")

    // Set row stop filter and specify you want to scan by row stop
    this.copy(scan = scan.setStopRow(rowStop.getBytes(utf8Encoding)), filterType = FilterType.RowStartAndStop)
  }

  /**
    * Set the number of rows (or parts of row) for caching will be passed to scanners. If not set, the default setting '1' will apply.
    * For example, setting this value to 500, will transfer 500 rows (or parts of rows) at a time to the client to be processed.
    * Put attention: Higher caching values will enable faster scanners but will use more memory.
    */
  @throws(classOf[IllegalArgumentException])
  def setCaching(caching: Int) = {
    require(Option(caching).nonEmpty && caching > 0, "Caching must be greater than zero")
    require(scan.getCaching == -1, "Caching has already been set")

    scan.setCaching(caching)
    this.copy()
  }

  /**
    * Set the maximum number of values to return for each call to next(),
    * so if you have 250 columns, batch of 100 would give your iterator:
    * Iteration 1: columns 0 -99
    * Iteration 2: columns 100 - 199
    * Iteration 3: columns 200 - 249
    */
  @throws(classOf[IllegalArgumentException])
  def setBatchSize(batchSize: Int) = {
    require(Option(batchSize).nonEmpty && batchSize > 0, "Batch size must be greater than zero")
    require(scan.getBatch == -1, "Batch size has already been set")

    scan.setBatch(batchSize)
    this.copy()
  }

  /**
    * Sets HBase configurations. For example, you might want to change the number of retries if you failed connect to HBase host.
    */
  @throws(classOf[IllegalArgumentException])
  def addConfiguration(config: Map[String, String]) = {
    require(Option(config).nonEmpty, "Invalid Configuration options")

    // Adds HBase configuration
    config.foreach((conf) => {
      val name = conf._1
      val value = conf._2
      hBaseConf.set(name, value)
    })

    this.copy()
  }

  /**
    * Returns a rdd of products, which represent rows in HBASE, according to what you query.
    */
  @throws(classOf[IllegalArgumentException])
  def load()(implicit sc: SparkContext): RDD[Product] = {

    val tableName = hBaseConf.get(TableInputFormat.INPUT_TABLE)
    require(Option(tableName).nonEmpty && tableName.nonEmpty, "Table name must be specified")
    require(Option(columns).nonEmpty && columns.nonEmpty, "Columns must be specified")
    require(Option(sc).nonEmpty, "Spark Context must be implicit specified")

    // Gets HBase url from the spark configuration
    val hBaseHostUrl = sc.getConf.get(hBaseHostKey)

    require(Option(hBaseHostUrl).nonEmpty && hBaseHostUrl.nonEmpty, "HBase url must be specified")

    // Configure HBase host
    hBaseConf.set(HConstants.ZOOKEEPER_QUORUM, hBaseHostUrl)

    /**
      * Write the given scan into a Base64 encoded string
      *
      * @return The scan saved in a Base64 encoded string
      *         $throws IOException
      */
    @throws(classOf[IOException])
    def convertScanToString: String = {

      // Convert a client Scan to a protocol buffer scan
      val proto = ProtobufUtil.toScan(scan)
      Base64.encodeBytes(proto.toByteArray)
    }

    // Set HBase configuration to read according to the base-64 encoded scanner
    hBaseConf.set(TableInputFormat.SCAN, convertScanToString)

    // Load a RDD of (row Key, row Result) tuples from the table
    val hBaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    // transform (row key, row result) tuples into an RDD of results
    val resultRDD = hBaseRDD.map(_._2)

    // transform into a RDD of (RowKey, ColumnValues) tuples
    resultRDD.map(result => {

      val rowKey = Bytes.toString(result.getRow)
      val values = columns.map(column => {
        val columnFamily = column._1
        val qualifier = column._2
        Bytes.toString(result.getValue(columnFamily.getBytes(utf8Encoding), qualifier.getBytes(utf8Encoding)))
      })

      // Create Tuple from array of strings
      def toTuple(params: Array[String]): Product = {
        val tupleClass = Class.forName("scala.Tuple" + params.length)
        tupleClass.getConstructors.apply(0).newInstance(params: _*).asInstanceOf[Product]
      }

      toTuple(Array(rowKey) ++ values)
    })
  }
}