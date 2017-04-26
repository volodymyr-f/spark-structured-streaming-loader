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
  val day = "day"
  val hour = "hour"
  val lang = "lang"
  val page = "page"
  val lastRead = "last_read"
  val lastUpd = "last_upd"
  val lastCommitMessageText = "last_commit_message_text"
  val hits = "hits"
  val size = "size"  

  val messageSchema = StructType(Seq(
    StructField(mId, IntegerType, true),
    StructField(tenantId, IntegerType, true),
    StructField(day, StringType, true),
    StructField(hour, IntegerType, true),
    StructField(lang, StringType, true),
    StructField(page, StringType, true),
    StructField(lastRead, StringType, true),
    StructField(lastUpd, StringType, true),
    StructField(lastCommitMessageText, StringType, true),
    StructField(hits, IntegerType, true),
    StructField(size, IntegerType, true)  
  ))
  
    def split(delimeter: String, value: String): String = {
      val pos = 1
      val langSplit = value.split(delimeter)
      if (langSplit.length > pos) {
       return  langSplit(pos)
      }
      return ""
    }

  val log: Logger = LogManager.getLogger("Streaming Kafka Consumer")

  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName("StructuredKafkaWordCount")
    .config("spark.cassandra.connection.host", "192.168.33.11")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()

  val connector = CassandraConnector.apply(spark.sparkContext.getConf)

  def main(args: Array[String]) {

    import spark.implicits._
    val dataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.33.10:9092")
      .option("group.id", "hour_consumer")
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
          println("Processing row: " + value)

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
  
    def insert(id: Int, tenantId: Int, day: String, hour: Int, lang: String, page: String, lastRead: Timestamp,lastUpd: Timestamp,  lastCommitMessageText: String, hits: Int, size: Int): String = {
 
    val langSplit = split(".", lang)
    val pageSplit = split(":", page)
 
    s"""insert into loader.page_count_hourly (m_id, tenant_id, day, hour, lang, last_commit_message, last_read, last_upd, page, project_name, project_type, req_count, size)
       values($id, $tenantId,'$day',$hour,'$lang','$lastCommitMessageText', '$lastRead', '$lastUpd', '$page', '$langSplit', '$pageSplit', $hits, $size)"""
  }

  private def insertRawRow(row: Row) = {
    connector.withSessionDo { session =>
      session.execute(insert(row.getAs(mId), row.getAs(tenantId), row.getAs(day), row.getAs(hour), row.getAs(lang), row.getAs(page), getTimeStamp(row.getAs(lastRead)), getTimeStamp(row.getAs(lastUpd)), row.getAs(lastCommitMessageText), row.getAs(hits), row.getAs(size)))
    }
  }

}