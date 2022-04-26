import algorithm.{Algorithms, TreeConvert}
import dataStructure.Data
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.util

object TrainModel {
  def main(args: Array[String]): Unit = {
    val ratio = 0.03
    val treeNum = 100
    // load data from database
    val sparkSession = SparkSession.builder.appName("anomaly detection").master("local[*]").getOrCreate()
    import sparkSession.implicits._

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/examples?autoReconnect=true&useSSL=false&rewriteBatchedStatements=true"
    val user = "root"
    val pas = "82831261"

    val dataframeAll = sparkSession.read.format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", "anomaly")
      .option("user", user)
      .option("password",pas)
      .load().select("v1", "v2", "v3", "v4", "v5", "class")//.limit(1000)

    val dataframe = dataframeAll.select("v1", "v2", "v3", "v4", "v5")

    val subTreeSize = (ratio * dataframe.count()).toInt
    dataframe.show(20)

    println("Total number of records: " + dataframe.count()) // 284807

    // sample dataset
    val schema = StructType(Array(
      StructField("index", IntegerType),
      StructField("values", StringType)
    ))
    var sampleResults = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)

    sampleResults.show()

    for (i <- 0 until treeNum) {
      val valueString = dataframe.sample(ratio).collect().mkString("|")
      val cacheDF = Seq((i+1, valueString)).toDF("index", "values")
      sampleResults = sampleResults.union(cacheDF)
    }

    // train model ----- node index, attribute,

    val treeSchema = StructType(Array(
      StructField("id", IntegerType),
      StructField("tree", StringType)
    ))

    val tree = sampleResults.mapPartitions(partition => {
      partition.map(data => {
        val rows = data(1).toString.split("\\|")
        val inputs = new util.ArrayList[Data]()
        for (i <- 0 until rows.length) {
          val row = rows(i).slice(1,rows(i).length-1).split(",")
          val singleRow = new util.ArrayList[java.lang.Double]()
          for (j <- 0 until row.length) {
            singleRow.add(row(j).toDouble)
          }
          val data = new Data(singleRow)
          inputs.add(data)
        }
        val treeHeight = Math.ceil(Math.log(inputs.size())/Math.log(2)).toInt
        val isolationForest = new Algorithms
        val treeNode = isolationForest.iTree(inputs, 0, treeHeight, 0)
        val convert = new TreeConvert;
        val trees = convert.getTrees(treeNode)

        Row(data(0), trees)
      })
    })(RowEncoder(treeSchema))

    tree.show(20)

    tree.write.mode("overwrite")
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "tree") //表名
      .option("user", user)
      .option("password", pas)
      .option("batchsize", "1000")
      .option("truncate", "true")
      .save()

  }



}
