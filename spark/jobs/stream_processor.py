from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import logging

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Input data schema
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("previous_close", FloatType(), True),
    StructField("change", FloatType(), True),
    StructField("change_percent", StringType(), True),
    StructField("timestamp", StringType(), True)
])

def create_spark_session():
    """Create and configure the Spark session"""
    return SparkSession.builder \
        .appName("Portfolio Risk Analyzer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .getOrCreate()

def process_stream(spark):
    """Process the data stream from Kafka"""
    
    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "financial_prices") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Decode JSON messages
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Add processing timestamp
    enriched_df = parsed_df.withColumn(
        "processing_time", 
        current_timestamp()
    ).withColumn(
        "returns",
        (col("price") - col("previous_close")) / col("previous_close")
    )
    
    # Show data in console (for debugging)
    query = enriched_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    logger.info("Spark Streaming started!")
    logger.info("Listening on: kafka:9092/topic: financial_prices")
    
    return query

if __name__ == "__main__":
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        query = process_stream(spark)
        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Streaming stopped")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
    finally:
        spark.stop()