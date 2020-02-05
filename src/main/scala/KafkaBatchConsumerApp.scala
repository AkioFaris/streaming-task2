import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object KafkaBatchConsumerApp extends App {

  val url = getClass.getResource("config.properties")
  val properties: Properties = new Properties()
  properties.load(Source.fromURL(url).bufferedReader())

  val props: Properties = new Properties()
  props.put("bootstrap.servers", properties.getProperty("broker_host_port"))

  val sparkSession = SparkSession.builder
    .appName("KafkaBatchConsumerApp")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  val df = sparkSession
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", properties.getProperty("broker_host_port"))
    .option("subscribe", properties.getProperty("topic"))
    .option("failOnDataLoss", false)
    .load()

  import sparkSession.implicits._

  df
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]
    .map(pair => pair._2.replaceAll("[\"\']", ""))
    .write
    .option("escapeQuotes","true")
    .mode(SaveMode.Overwrite)
    .csv(properties.getProperty("output_path_batch"))

  sparkSession.stop()
}
