# XnodeProjectOne
Project Proposal: Intelligent Data Pipelines for Financial Asset Management 
Foundation Setup:

Downloaded Packages for Spark and Kafka. Installed necessary packages, including PySpark
Created an account on Snowflake and set up a database for this project
Set up the consumer and producer consoles on Kafka
Configured Prometheus and Grafana using Homebrew
Set up Great Expectations Core and set up an expectation for my Snowflake table. 



Pipeline Development:



Data from CSV File:
Found 2,847 rows of Data from Data.gov on End-of-Day Pricing from NYSE Stocks. (I was not able to find sufficient data to match 1 TB per day, as it interfered with my computer storage.)
Column Names: Symbol, Date, Open, High, Low, Close, Volume. (Given time constraints, I was only able to find data with 1 timestamp: Date, and not bitemporal data with 2 timestamps.)
Downloaded data as a CSV File
Apache Kafka:
Configured basic settings, including adjusting the default data directory of the broker Zookeeper, and adjusting the kafka-logs in server properties
Started both Zookeeper and Server in my Kafka Folder:
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
Created Kafka Topic: earnings_topic
Set up the Consumer and Producer Kafka System
Ingested my dataset into Kafka from CSV, through Python3 (producers.py)
Apache Spark:
Used Spark for Data Reshaping and Data Deduplication
Built a schema with all Column Names and Data types to be sent to the Snowflake database
Dropped duplicates of the dataframe using pyspark
Snowflake
Wrote a parsed dataframe on the Snowflake account
(Code was done in Python3 - consumers.py)
Great Expectations Core
Connected the data source to Snowflake
Set a preset expectation
Due to time constraints, I was unable to validate this expectation with my batch of data; therefore, I left it at declaring the expectation. 
Prometheus and Grafana
Used for processing latencies and triggering alerts.
Configured Prometheus and Grafana; however was not able to implement it in the pipeline due to time constraints. 


