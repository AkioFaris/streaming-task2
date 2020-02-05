import java.util.Properties

import org.apache.spark.sql.SparkSession

import scala.io.Source

object KafkaStructuredConsumerApp extends App {

  val url = getClass.getResource("config.properties")
  val properties: Properties = new Properties()
  properties.load(Source.fromURL(url).bufferedReader())

  val props: Properties = new Properties()
  props.put("bootstrap.servers", properties.getProperty("broker_host_port"))

  val sparkSession = SparkSession.builder
    .appName("KafkaStructuredConsumerApp")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  val df = sparkSession
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", properties.getProperty("broker_host_port"))
    .option("subscribe", properties.getProperty("topic"))
    .option("checkpointLocation", "home/maria_dev/hw-spark/checkpoints/checkpoint")
    .option("failOnDataLoss", false)
    .option("startingOffsets", "earliest")
    .load()

  import sparkSession.implicits._

  val block_sz = 1024
  val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]
    .map(pair => pair._2.replaceAll("\"", ""))
    .writeStream
    .format("csv")
    .option("path", properties.getProperty("output_path_struct"))
    .option("parquet.block.size", block_sz)
    .option("checkpointLocation", properties.getProperty("checkpoint_path"))
    .start()

  while (query.isActive) {
    val msg = query.status.message
    if (!query.status.isDataAvailable && !query.status.isTriggerActive && !msg.equals("Initializing sources")) {
      query.stop()
    }
  }
  sparkSession.stop()
}
