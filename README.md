# spark-structured-streaming-loader
A simple Spark Structured Streaming application to consume a structured stream from Kafka.

  ./bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0 \
  --class com.loader.KafkaConsumer \
  --master local[*] \
   /vagrant/sparkStructuredStreamingLoader.jar \

