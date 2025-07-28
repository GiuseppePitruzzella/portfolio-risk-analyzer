#!/usr/bin/env python3
"""
Spark Stream Processor per Portfolio Risk Analyzer
Questo script legge i dati degli ETF da Kafka, li processa e prepara
le basi per il calcolo delle metriche di rischio.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

class ETFStreamProcessor:
    def __init__(self):
        """Inizializza il processore Spark Streaming"""
        self.app_name = "Portfolio Risk Analyzer - Stream Processor"
        self.spark = None
        self.kafka_servers = "kafka:29092"
        self.kafka_topic = "financial_prices"
        self.checkpoint_location = "/tmp/spark-checkpoints"
        
    def create_spark_session(self):
        """Crea e configura la sessione Spark"""
        try:
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location) \
                .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            # Imposta il log level per ridurre il rumore
            self.spark.sparkContext.setLogLevel("WARN")
            
            print(f"✅ Spark Session creata: {self.app_name}")
            print(f"🌐 Spark UI disponibile su: http://localhost:8081")
            
            return True
            
        except Exception as e:
            print(f"❌ Errore nella creazione della Spark Session: {e}")
            return False
    
    def define_schema(self):
        """Definisce lo schema per i dati degli ETF"""
        return StructType([
            StructField("symbol", StringType(), True),
            StructField("current_price", DoubleType(), True),
            StructField("change", DoubleType(), True),
            StructField("percent_change", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("open", DoubleType(), True),
            StructField("previous_close", DoubleType(), True),
            StructField("intraday_volatility", DoubleType(), True),
            StructField("intraday_range", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("data_source", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("doc_id", StringType(), True)
        ])
    
    def read_from_kafka(self):
        """Legge i dati dal topic Kafka"""
        try:
            # Schema per i dati ETF
            etf_schema = self.define_schema()
            
            # Legge dal topic Kafka
            kafka_stream = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_servers) \
                .option("subscribe", self.kafka_topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            # Deserializza i dati JSON
            parsed_stream = kafka_stream.select(
                col("key").cast("string").alias("kafka_key"),
                col("timestamp").alias("kafka_timestamp"),
                col("partition"),
                col("offset"),
                from_json(col("value").cast("string"), etf_schema).alias("data")
            ).select(
                col("kafka_key"),
                col("kafka_timestamp"),
                col("partition"),
                col("offset"),
                col("data.*")
            )
            
            print("✅ Stream Kafka configurato correttamente")
            return parsed_stream
            
        except Exception as e:
            print(f"❌ Errore nella lettura da Kafka: {e}")
            return None
    
    def add_technical_indicators(self, df):
        """Aggiunge indicatori tecnici semplici compatibili con Structured Streaming"""
        
        enhanced_df = df.withColumn("processing_time", current_timestamp()) \
            .withColumn("price_timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("date_only", to_date(col("processing_time"))) \
            .withColumn("hour_bucket", date_trunc("hour", col("processing_time"))) \
            .withColumn(
                # Range percentuale dal minimo al massimo (intraday volatility)
                "price_range_pct",
                when(col("low") > 0, (col("high") - col("low")) / col("low") * 100)
                .otherwise(0.0)
            ) \
            .withColumn(
                # Distanza dal prezzo di apertura
                "distance_from_open",
                when(col("open") > 0, 
                     (col("current_price") - col("open")) / col("open") * 100)
                .otherwise(0.0)
            ) \
            .withColumn(
                # Flag per identificare movimenti anomali
                "is_significant_move",
                when(abs(col("percent_change")) > 2.0, True).otherwise(False)
            ) \
            .withColumn(
                # Categoria di prezzo (alto, medio, basso basato sul range giornaliero)
                "price_category",
                when((col("low") > 0) & (col("high") > col("low")),
                     when(col("current_price") > (col("high") + col("low")) / 2 + (col("high") - col("low")) * 0.25, "high")
                     .when(col("current_price") < (col("high") + col("low")) / 2 - (col("high") - col("low")) * 0.25, "low")
                     .otherwise("medium"))
                .otherwise("unknown")
            ) \
            .withColumn(
                # Volatilità intraday assoluta
                "volatility_score",
                when(col("open") > 0, abs(col("percent_change")))
                .otherwise(0.0)
            ) \
            .withColumn(
                # Forza del movimento (basato su volume implicito dal range)
                "movement_strength",
                when((col("low") > 0) & (col("high") > col("low")),
                     (col("high") - col("low")) / ((col("high") + col("low")) / 2) * 100)
                .otherwise(0.0)
            )
        
        return enhanced_df
    
    def filter_and_validate_data(self, df):
        """Filtra e valida i dati in ingresso"""
        filtered_df = df.filter(
            # Filtra solo dati validi (usa & invece di and)
            (col("current_price") > 0) &
            (col("symbol").isNotNull()) &
            (col("symbol").isin(["SPY", "QQQ", "IWM"])) &
            # Filtra prezzi ragionevoli (evita dati corrotti)
            (col("current_price") > 1) &
            (col("current_price") < 1000)
        ).withColumn(
            # Aggiungi flag di qualità dei dati
            "data_quality_score",
            when(
                (col("high") >= col("current_price")) &
                (col("low") <= col("current_price")) &
                (col("high") >= col("low")) &
                (col("open") > 0), 1.0
            ).otherwise(0.5)
        )
        
        return filtered_df
    
    def create_market_summary(self, df):
        """Crea un summary di mercato aggregato usando time windows"""
        
        # Finestra temporale per aggregazioni (ultimi 5 minuti)
        time_window = "5 minutes"
        
        market_summary = df \
            .withWatermark("processing_time", "10 minutes") \
            .groupBy(
                window(col("processing_time"), time_window),
                col("symbol")
            ) \
            .agg(
                avg("current_price").alias("avg_price"),
                max("current_price").alias("max_price"),
                min("current_price").alias("min_price"),
                avg("percent_change").alias("avg_change"),
                stddev("current_price").alias("price_std"),
                avg("price_range_pct").alias("avg_volatility"),
                max("volatility_score").alias("max_volatility"),
                count("*").alias("data_points"),
                max("processing_time").alias("last_update"),
                # Conteggio movimenti significativi
                sum(when(col("is_significant_move"), 1).otherwise(0)).alias("significant_moves"),
                # Distribuzione categorie prezzo
                sum(when(col("price_category") == "high", 1).otherwise(0)).alias("high_price_count"),
                sum(when(col("price_category") == "low", 1).otherwise(0)).alias("low_price_count")
            ) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .withColumn(
                # Calcola il range percentuale della finestra
                "window_range_pct",
                when(col("min_price") > 0, 
                     (col("max_price") - col("min_price")) / col("min_price") * 100)
                .otherwise(0.0)
            ) \
            .withColumn(
                # Score di attività per il simbolo
                "activity_score",
                col("significant_moves") * 10 + col("data_points")
            ) \
            .drop("window")
        
        return market_summary
    
    def start_processing(self):
        """Avvia il processing dei dati streaming"""
        if not self.create_spark_session():
            return
        
        print(f"🚀 Avvio ETF Stream Processor...")
        print(f"📺 Kafka Topic: {self.kafka_topic}")
        print(f"🔗 Kafka Servers: {self.kafka_servers}")
        
        # Leggi dati da Kafka
        raw_stream = self.read_from_kafka()
        if raw_stream is None:
            return
        
        # Applica filtri e validazioni
        validated_stream = self.filter_and_validate_data(raw_stream)
        
        # Aggiungi indicatori tecnici
        enhanced_stream = self.add_technical_indicators(validated_stream)
        
        # Crea summary di mercato
        market_summary = self.create_market_summary(enhanced_stream)
        
        # Query 1: Output principale con dati arricchiti (per debug e monitoraggio)
        query1 = enhanced_stream.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 5) \
            .trigger(processingTime="30 seconds") \
            .queryName("enhanced_etf_data") \
            .start()
        
        # Query 2: Market Summary (aggregazioni temporali)
        query2 = market_summary.writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 10) \
            .trigger(processingTime="60 seconds") \
            .queryName("market_summary") \
            .start()
        
        print("✅ Stream processing avviato!")
        print("📊 Due query attive:")
        print("   1. Enhanced ETF Data - ogni 30 secondi")
        print("   2. Market Summary - ogni 60 secondi")
        print("🛑 Premi Ctrl+C per fermare")
        
        try:
            # Aspetta che le query finiscano
            query1.awaitTermination()
            query2.awaitTermination()
        except KeyboardInterrupt:
            print("\n🛑 Interruzione ricevuta. Fermando le query...")
            query1.stop()
            query2.stop()
        except Exception as e:
            print(f"❌ Errore durante il processing: {e}")
        finally:
            if self.spark:
                self.spark.stop()
            print("✅ Stream processor terminato")

def main():
    """Funzione principale"""
    print("🚀 Portfolio Risk Analyzer - Spark Stream Processor")
    print("="*60)
    
    processor = ETFStreamProcessor()
    processor.start_processing()

if __name__ == "__main__":
    main()