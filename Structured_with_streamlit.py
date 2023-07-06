from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, when, window
from pyspark.sql.types import StructType, StringType, LongType,TimestampType
import streamlit as st



# We set the environment variable PYSPARK_SUBMIT_ARGS to include the necessary
# package dependency for Kafka integration in the Spark application.
import os
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 pyspark-shell"

bootstrap_server = "localhost:9092"
consumer_topic = "network-traffic"
producer_topic = "network-traffic-producer"



# Create SparkSession
spark = SparkSession.builder \
    .appName("NetworkTrafficAnomalyDetection") \
    .getOrCreate()

# Define the schema for incoming JSON messages
schema = StructType() \
    .add("source_ip", StringType()) \
    .add("destination_ip", StringType()) \
    .add("protocol", StringType()) \
    .add("bytes", LongType())\
    .add("timestamp",TimestampType())

# Create a streaming DataFrame that reads from a Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_server) \
    .option("subscribe", consumer_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Convert the value column from binary to string
df = df.withColumn("value", df["value"].cast("string"))

# Parse the JSON data and select required columns
parsed_df = df \
    .select(from_json(df["value"], schema).alias("data")) \
    .select("data.source_ip", "data.destination_ip", "data.protocol", "data.bytes","data.timestamp")

# Perform network traffic analysis with sliding window
anomaly_df = parsed_df \
    .withColumn("is_anomaly", when(col("bytes") > 10000, 1).otherwise(0)) \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window("timestamp", "10 minutes", "5 minutes"), col("source_ip")) \
    .agg(sum("is_anomaly").alias("anomaly_count"))

# Select relevant columns for producing to Kafka
output_df = anomaly_df \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("source_ip"),
        col("anomaly_count").cast("string").alias("value")
    )

def main():
    st.title("Network Traffic Anomaly Detection")
    st.header("Real-time Streaming Data")

    # Create a Streamlit DataFrame for visualizing the streaming data
    streamlit_df = st.dataframe(output_df.limit(10).toPandas())
    streamlit_df.dropna(inplace =True)

    # Continuously update the Streamlit DataFrame as new data arrives
    stream = parsed_df.writeStream \
        .format("memory") \
        .queryName("streaming_data") \
        .outputMode("append") \
        .start()

    while True:
        spark.sql("SELECT * FROM streaming_data").toPandas()
        streamlit_df.dataframe(output_df.limit(10).toPandas())

if __name__ == "__main__":
    main()
