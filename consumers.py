# Import necessary libraries to use PySpark in Kafka consumer
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import expr, split
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import *

#Define the Snowflake connection options to link with my Account
snowflake_options = {
    "sfURL": "MIZRUDP-NN16547.snowflakecomputing.com",
    "sfUser": "ARJUNR9206",
    "sfPassword": "____________",
    "sfDatabase": "ARJUN_TEST",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH"
}
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "earnings_topic"


# Start and initialize a Spark Session
spark = SparkSession.builder \
    .appName("KafkaToSparkETL") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,net.snowflake:snowflake-jdbc:3.24.2,net.snowflake:spark-snowflake_2.13:3.1.1") \
    .getOrCreate()

# add all Column names and types to structure
schema = StructType() \
    .add("Symbol", StringType()) \
    .add("Date", StringType()) \
    .add("Open", StringType()) \
    .add("High", StringType()) \
    .add("Low", StringType()) \
    .add("Close", StringType()) \
    .add("Volume", StringType()) \
    
# take in the raw data from Kafka (producer) - reading from Kafka topic
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

#Defined a function that writes to Snowflake (handling exceptions)
def safe_write_to_snowflake(batch_df, batch_id):
    try:
        batch_df.write \
            .format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", "NYSE_SHORTENED") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"Error writing batch {batch_id} to Snowflake: {str(e)}")

#Parsed data into JSON format using the schema defined above
df_json = df_raw.select(from_json(col("value"), schema).alias("data")).select("data.*")

#Deduplicated the data and removed all null rows in the dataset
df_deduplicated = df_json.dropDuplicates(subset = ['Symbol'])
df_clean = df_deduplicated.na.drop()

#Writing the data to Snowflake and saving this in checkpoint location
query = df_clean.writeStream \
    .foreachBatch(safe_write_to_snowflake) \
    .trigger(processingTime="15 seconds") \
    .option("checkpointLocation", "/tmp/snowflake/checkpoint") \
    .start().awaitTermination()

