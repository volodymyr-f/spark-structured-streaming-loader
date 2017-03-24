package com.loader

import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType };
import org.apache.spark.sql.streaming._
import scala.concurrent.duration._
import org.apache.spark.sql._
import com.datastax.spark.connector.cql.CassandraConnector
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.log4j.LogManager
import java.sql.Timestamp
import java.text.{ DateFormat, SimpleDateFormat }
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

/**
 * This is a simple implementation of Kafka Consumer
 * That takes value as json and inserts into Cassandra table
 */
object KafkaConsumer {

  val mId = "m_id"
  val tenantId = "tenant_id"
  val prevLink = "prev_link"
  val currLink = "curr_link"
  val timestampClicked = "timestamp_clicked"
  val timestampReceived = "timestamp_received"
  val message = "message"
  val eventType = "type"

  val messageSchema = StructType(Seq(
    StructField(mId, IntegerType, true),
    StructField(tenantId, IntegerType, true),
    StructField(prevLink, StringType, true),
    StructField(currLink, StringType, true),
    StructField(timestampClicked, StringType, true),
    StructField(timestampReceived, StringType, true),
    StructField(message, StringType, true),
    StructField(eventType, StringType, true)))

  def insert(id: Int, tenantId: Int, prevLink: String, currLink: String, timestampСlicked: Timestamp, timestampReceived: Timestamp, message: String, messageType: String): String = s"""
       insert into loader.events_raw (m_id, tenant_id, prev_link, curr_link, timestamp_clicked,timestamp_received,message,type)
       values($id,$tenantId,'$prevLink','$currLink','$timestampСlicked','$timestampReceived','$message','$messageType')"""

  val log: Logger = LogManager.getLogger("Streaming Kafka Consumer")

  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName("StructuredKafkaWordCount")
    .config("spark.cassandra.connection.host", "192.168.33.10")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()

  val connector = CassandraConnector.apply(spark.sparkContext.getConf)

  def main(args: Array[String]) {

    import spark.implicits._
    val dataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "loader_1")
      .load()
      .toDF()
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", messageSchema).as("data"))
      .select("data.*")
    dataFrame.printSchema()

    val query = dataFrame.writeStream.trigger(ProcessingTime(5.seconds))
      .foreach(new ForeachWriter[Row] {

        override def process(value: Row): Unit = {
          println("Processing row: ")
          insertRawRow(value)
        }

        override def close(errorOrNull: Throwable): Unit = {
        }

        override def open(partitionId: Long, version: Long): Boolean = {
          true
        }
      }).start()
    query.awaitTermination() // Wait for the computation to terminate (manually or due to any error)

  }

  def getTimeStamp(timeStr: String): Timestamp = {
    val formatter = new DateTimeFormatterBuilder().appendPattern("yyyy/MM/dd HH:mm:ss").toFormatter()
    val date: Option[Timestamp] = {
      Some(Timestamp.valueOf(LocalDateTime.parse(timeStr, formatter)))
    }
    date.getOrElse(Timestamp.valueOf(timeStr))
  }

  private def insertRawRow(row: Row) = {
    connector.withSessionDo { session =>
      session.execute(insert(row.getAs(mId), row.getAs(tenantId), row.getAs(prevLink), row.getAs(currLink), getTimeStamp(row.getAs(timestampClicked)), getTimeStamp(row.getAs(timestampReceived)), row.getAs(message), row.getAs(eventType)))
    }
  }

}