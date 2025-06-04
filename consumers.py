# Import necessary libraries to use PySpark in Kafka consumer
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Start and initialize a Spark Session
spark = SparkSession.builder \
    .appName("KafkaToSparkETL") \
    .getOrCreate()

# add all Column names and types to structure
schema = StructType() \
    .add("Symbol", StringType()) \
    .add("Date", TimestampType()) \
    .add("Open", StringType()) \
    .add("High", StringType()) \
    .add("Low", StringType()) \
    .add("Close", StringType()) \
    .add("Volume", StringType()) \
    
# take in the raw data from Kafka (producer) - reading from Kafka topic
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "earnings_topic") \
    .load()

#Parse the raw data by DEDUPLICATING the data - feature of spark
df_parsed = df_raw.dropDuplicates() 

#Writing the parsed data to Snowflake Project database
df_parsed.writeStream \
    .format("snowflake") \
    .option("sfURL", "mizrudp-nn16547.snowflakecomputing.com") \
    .option("sfUser", "arjunr9206") \
    .option("sfPassword", "College11!!!!!") \
    .option("sfDatabase", "analytics") \
    .option("sfSchema", "public") \
    .option("sfWarehouse", "compute_wh") \
    .option("dbtable", "employee_earnings") \
    .option("checkpointLocation", "/tmp/checkpoints/employee_earnings") \
    .start()
