import algorithm.{Analysis, Calculate, TreeConvert}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import java.sql.DriverManager.getConnection
import java.sql.PreparedStatement
import java.util

object Validate {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder.appName("anomaly detection").master("local[*]").getOrCreate()
    import sparkSession.implicits._

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/examples?autoReconnect=true&useSSL=false&rewriteBatchedStatements=true"
    val user = "root"
    val pas = "82831261"

    val batchSize = 500
    val parts = 5
    val ratio = 0.03

    val totalRow: Long = sparkSession.read.format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", "anomaly")
      .option("user", user)
      .option("password",pas)
      .load().count()

    val subTreeSize: Int = (ratio * totalRow).toInt

    val tree: DataFrame = sparkSession.read.format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", "tree")
      .option("user", user)
      .option("password",pas).load()

    val treeBroadCast: Broadcast[Array[Row]] = sparkSession.sparkContext.broadcast(tree.select("tree").collect())
    val treesValue: Array[Row] = treeBroadCast.value

    var start = 0
    while (start < totalRow) {
      val dataframeAll = sparkSession.read.format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("dbtable", "(SELECT v1, v2, v3, v4, v5, class, num FROM orderedAnomaly WHERE num > " + start.toString + " and class =1) scores")
        .option("user", user)
        .option("password",pas)
        .load().limit(batchSize)//.select("v1", "v2", "v3", "v4", "v5", "class", "num").where("num > " + start.toString).limit(batchSize) //TODO add index

//      val dataframeAll = sparkSession.read.format("jdbc").options(Map("url" -> url, "dbtable" -> "(SELECT s.*,u.name FROM t_score s JOIN t_user u ON s.id=u.score_id) t_score")).load()

      dataframeAll.show(20)


      start += batchSize

      val scoreSchema = StructType(Array(
        StructField("features", StringType),
        StructField("scoreList", StringType),
        StructField("averagePath", DoubleType),
        StructField("score", DoubleType),
        StructField("label", IntegerType)
      ))

      val score = dataframeAll.mapPartitions(partition => {
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

//      score.repartition(parts).write.mode("append")
//        .format("jdbc")
//        .option("url", url)
//        .option("dbtable", "score") //表名
//        .option("user", user)
//        .option("password", pas)
//        .option("batchsize", "200")
//        .option("truncate", "true")
//        .option("numPartitions", 10)
//        .save()

      val sql=
        s"""
           |insert into score (features, scoreList, averagePath, score, label) values(?, ?, ?, ?, ?)
           |""".stripMargin


      score.repartition(parts).foreachPartition(part => {
        insert(sql, part, url, user, pas, 1000)
      })

    }


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
