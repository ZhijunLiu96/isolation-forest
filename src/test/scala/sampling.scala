import algorithm.{Algorithms, Analysis, Calculate, TreeConvert}
import dataStructure.Data
import org.apache.spark
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.netbeans.modules.db.util.JdbcUrl

import java.sql.DriverManager.getConnection
import java.sql.PreparedStatement
import java.util.Properties
import java.util


object sampling {


  def main(args: Array[String]): Unit = {

    val ratio = 0.03

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

    for (i <- 0 to 10) {
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

    // broadcast trees info
    val treeBroadCast = sparkSession.sparkContext.broadcast(tree.select("tree").collect())
    val treesValue = treeBroadCast.value

    // predict all instance

    val scoreSchema = StructType(Array(
      StructField("features", StringType),
      StructField("scoreList", StringType),
      StructField("averagePath", DoubleType),
      StructField("score", DoubleType),
      StructField("label", IntegerType)
    ))

    val score = dataframeAll.repartition(10).mapPartitions(partition => {

      partition.map(row => {
        val instances = new util.ArrayList[java.lang.Double]()
        for (i <- 0 to 4) {
          instances.add(row(i).toString.toDouble)
        }
        val scores = new util.ArrayList[java.lang.Double]()
        // parse trees
        treesValue.foreach(data => {
          val treeAlgo = new TreeConvert
          val rootNode = treeAlgo.string2tree(data(0).toString)
          // calculate scores
          val singleScore = Analysis.PathLength(instances, rootNode, 0)
          scores.add(singleScore)
        })
        val avgScore = Calculate.calculateAverage(scores)
        // 参数是子树的大小即 sampling size，异常值的score越大
        val anomalyScore = Calculate.calculate_anomalyScore(avgScore, subTreeSize)

        Row(instances.toString, scores.toString, avgScore, anomalyScore, row(5).toString.toInt)
      })


    })(RowEncoder(scoreSchema))

    score.show(20)



//    score.createOrReplaceTempView("output")
//    sparkSession.sql("select * from output where label=1").show()

    // TODO 查看结果

    //    val result = score.filter("label==1")
    //    result.show(20)

//    val jdbcUrl = "jdbc:mysql://localhost:3306/examples"
//    val connectionProperties = new Properties()
//    connectionProperties.put("user", "root")
//    connectionProperties.put("password", "82831261")
//    score.repartition(1).write.mode(SaveMode.Append).jdbc(jdbcUrl, "score", connectionProperties)

//    val sql=
//      s"""
//         |insert into score (features, scoreList, averagePath, score, label) values(?, ?, ?, ?, ?)
//         |""".stripMargin
//
//
//
//    score.repartition(10).foreachPartition(part => {
//      insert(sql, part, url, user, pas, 1000)
//    })

    score.write.mode("overwrite")
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "score") //表名
      .option("user", user)
      .option("password", pas)
      .option("batchsize", "1000")
      .option("truncate", "true")
      .save()

//    sparkSession.sql("SELECT * FROM scores WHERE label=1").show(20)

//    score.repartition(10).write.format("csv").save("file://Users//zhijunliu//IdeaProjects//IsolationForest//src//main//resources//multiple.csv")
  }

  def insert(sql: String, rows: Iterator[Row], jdbcUrl: String, jdbcUsername: String, jdbcPassword: String, maxBatchSize: Integer): Unit = {
    val conn = getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
    val pstat: PreparedStatement = conn.prepareStatement(sql)
    try {
      var size = 0
      while (rows.hasNext) {
        val row: Row = rows.next()
        val len = row.length
        for (i <- 0 until len) {
          pstat.setObject(i + 1, row.get(i))
        }
        pstat.addBatch()
        size += 1
        if (size % maxBatchSize == 0) {
          pstat.executeBatch()
          println("=======批量插入数据成功,数量是[{}]=======", size)
          size = 0
        }
      }
      if (size > 0) {
        pstat.executeBatch()
        println("=======批量插入数据成功,数量是[{}]=======", size)
      }
    } finally {
      pstat.close()
      conn.close()
    }
  }

}
