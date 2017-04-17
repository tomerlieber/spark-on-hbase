# spark-on-hbase
This project lets your Apache Spark application interact with Apache HBase using simple API.

## Table of contents
- [Including the library](#including-the-library)
- [Setting the HBase host](#setting-the-hbase-host)
- [Import implicits](#import-implicits)
- [Reading from HBase](#reading-from-hbase)
- [Writing to HBase](#writing-to-hbase)
- [Deleting from HBase](#deleting-from-hbase)


## Including the library
Currently the project is not available in any repository, so in order to use it few steps should be done:
1. Download the project.
2. Rebuild the project.
3. Add an empty jar artifact with 'spark-on-hbase' compile output and use an existing manifest.
4. Build artifact.
5. Add the output jar to your project as library and have fun!

## Setting the HBase host
The HBase host can be set in few ways:

1. Using the *scala* code:
    
    ```scala
    val sparkConf = new SparkConf().setMaster("local").set("spark.hbase.host", "xx.xxx.xxx.xx:1234")
    implicit val sc = new SparkContext(sparkConf)
    ```
    
2. Using the hbase-site.xml file **(Not implemented)**

## Import implicits
Before preform any action on HBase, import necessary implicits in order to be able to access the *HBaseHanlder* class.

```scala
import main.org.x.spark.hbase.Implicits._
```

## Reading from HBase

First, get the *HBaseReader* class. The function *toHBaseReader()* extends the *org.apache.spark.SparkContext* in order to do it.

```scala
sc.toHBaseReader
```

Second, select which column familes and qualifers to read from the table. The column family and the qualifier are separated by colon.

```scala
sc.toHBaseReader.select("cf1:q1", "cf2;q2")
```

Third, select which table you want to read from.

```scala
sc.toHBaseReader.select("cf1:q1", "cf2;q2").from("tableName")
```

Finally, use the function *load()* in order to get the data as a RDD collection from HBase.

```scala
sc.toHBaseReader.select("cf1:q1", "cf2;q2").from("tableName").load()
```

Other functions you may use are:
- setBatchSize(batchSize: Int) - Set the maximum number of values to return for each call to next().
- withRowStart(startRow: String) - The row key of the record you want to start reading from.
- withRowStop(startStop: String) - The row key of the record you want to stop reading from.
- withRowKeys(rowKeys: Seq[String]) - The row keys of the records you want to read from.
And some more...


## Writing to HBase
First, create or use a RDD collection where each element containing a row key and values of a record you want to write to HBase

```scala
val recordsToSave = sc.parallielize(0 to 9).map(rowKey => (rowKey, "value1", "value2", "value3"))
```

Second, get The *HBaseWriter* class. The function *toHBaseWriter()* extends a RDD of Tuples in order to do it.

```scala
recordsToSave.toHBaseWriter
```

Third, select which table you want to write to.

```scala
recordsToSave.toHBaseWriter.into("tableName")
```

Fourth, select which columns you want to write to. The column family and the qualifier are separated by colon. Put attention that each column should be a corresponding value in the record you want to write.

```scala
recordsToSave.toHBaseWriter.into("tableName").toColumns("cf1:q1", "cf2:q2", "cf3:q3")
```

Finally, use the function *save()* in order to write the records to HBase.

```scala
recordsToSave.toHBaseWriter.into("tableName").toColumns("cf1:q1", "cf2:q2", "cf3:q3").save()
```

## Deleting from HBase
First, create or use a *RDD[String]* of row keys of records you want to delete from HBase.

```scala    
val recordsToDelete = sc.parallelize(1 to 3) // TODO: check the the funcion parallelize here return rdd[string]
```

Second, get the *HBaseDelete* class. The function *toHBaseDelete()* extends a *RDD[String]* in order to do it.

```scala
recordsToDelete.toHBaseDelete
```

Third, select which table you want to delete from.

```scala
recordsToDelete.toHBaseDelete.from("tableName")
```

Finally, use the function *delete()* in order to delete the records from HBase.

```scala
recordsToDelete.toHBaseDelete.from("tableName").delete()
```
