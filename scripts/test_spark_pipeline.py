#!/usr/bin/env python3
"""
Test Script per verificare che la pipeline Spark funzioni correttamente
Prima di lanciare il processing continuo, testiamo la connettività
"""

import os
import time
import json
import subprocess
from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from dotenv import load_dotenv

load_dotenv()

def test_spark_connection():
    """Testa la connessione a Spark"""
    try:
        print("🔍 Testando connessione Spark...")
        
        spark = SparkSession.builder \
            .appName("Spark Connection Test") \
            .master("spark://localhost:7077") \
            .getOrCreate()
        
        # Test semplice
        test_df = spark.range(10).withColumn("doubled", col("id") * 2)
        result = test_df.collect()
        
        spark.stop()
        
        print("✅ Spark funziona correttamente!")
        print(f"   Test completato con {len(result)} righe")
        return True
        
    except Exception as e:
        print(f"❌ Errore Spark: {e}")
        return False

def test_kafka_spark_integration():
    """Testa l'integrazione Kafka-Spark con dati di esempio"""
    try:
        print("🔍 Testando integrazione Kafka-Spark...")
        
        # Prima verifichiamo che ci siano messaggi in Kafka
        consumer = KafkaConsumer(
            'financial_prices',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=5000,
            group_id='test_integration_group'
        )
        
        messages = []
        for message in consumer:
            messages.append(message)
            if len(messages) >= 3:  # Prendiamo solo i primi 3 per il test
                break
        
        consumer.close()
        
        if len(messages) == 0:
            print("⚠️  Nessun messaggio trovato in Kafka")
            print("💡 Aspetta che Logstash invii alcuni dati, poi riprova")
            return False
        
        print(f"✅ Trovati {len(messages)} messaggi in Kafka")
        
        # Ora testiamo che Spark possa leggere da Kafka
        # Usiamo il container Spark per evitare problemi di network
        import subprocess
        
        # Creiamo un test script molto semplice
        test_script = '''
import sys
sys.path.append("/opt/bitnami/spark/python")
sys.path.append("/opt/bitnami/spark/python/lib/py4j-0.10.9.7-src.zip")

from pyspark.sql import SparkSession

try:
    spark = SparkSession.builder \\
        .appName("Kafka Integration Test") \\
        .getOrCreate()
    
    # Test semplice di lettura batch da Kafka
    df = spark.read \\
        .format("kafka") \\
        .option("kafka.bootstrap.servers", "kafka:29092") \\
        .option("subscribe", "financial_prices") \\
        .option("startingOffsets", "earliest") \\
        .option("endingOffsets", "latest") \\
        .load()
    
    count = df.count()
    print(f"SUCCESS: {count} messages read from Kafka")
    
    spark.stop()
    
except Exception as e:
    print(f"ERROR: {str(e)}")
    sys.exit(1)
'''
        
        # Scriviamo il test script temporaneo
        with open('scripts/spark/kafka_test.py', 'w') as f:
            f.write(test_script)
        
        # Eseguiamo il test nel container Spark
        result = subprocess.run([
            'docker', 'compose', 'exec', '-T', 'spark-master',
            'spark-submit',
            '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
            '/opt/bitnami/spark/jobs/kafka_test.py'
        ], capture_output=True, text=True, timeout=60)
        
        # Cleanup
        if os.path.exists('scripts/spark/kafka_test.py'):
            os.remove('scripts/spark/kafka_test.py')
        
        if result.returncode == 0 and "SUCCESS:" in result.stdout:
            message_count = result.stdout.split("SUCCESS: ")[1].split(" messages")[0]
            print(f"✅ Kafka-Spark integration OK! Spark ha letto {message_count} messaggi")
            return True
        else:
            print(f"❌ Test Spark fallito:")
            print(f"   Return code: {result.returncode}")
            print(f"   Stdout: {result.stdout}")
            print(f"   Stderr: {result.stderr}")
            return False
        
    except subprocess.TimeoutExpired:
        print("❌ Test Spark timeout (> 60 secondi)")
        return False
    except Exception as e:
        print(f"❌ Errore integrazione Kafka-Spark: {e}")
        import traceback
        traceback.print_exc()
        return False

def send_test_message():
    """Invia un messaggio di test a Kafka"""
    try:
        print("🔍 Invio messaggio di test a Kafka...")
        
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )
        
        test_message = {
            "symbol": "TEST",
            "current_price": 100.50,
            "change": 1.25,
            "percent_change": 1.26,
            "high": 101.00,
            "low": 99.75,
            "open": 100.00,
            "previous_close": 99.25,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "data_source": "test",
            "data_type": "test_message"
        }
        
        future = producer.send('financial_prices', key='TEST', value=test_message)
        result = future.get(timeout=10)
        
        producer.close()
        
        print("✅ Messaggio di test inviato con successo!")
        print(f"   Partition: {result.partition}, Offset: {result.offset}")
        return True
        
    except Exception as e:
        print(f"❌ Errore invio messaggio test: {e}")
        return False

def run_comprehensive_test():
    """Esegue tutti i test in sequenza"""
    print("🚀 Portfolio Risk Analyzer - Test Pipeline Completa")
    print("="*60)
    
    tests = [
        ("Connessione Spark", test_spark_connection),
        ("Invio messaggio test", send_test_message),
        ("Integrazione Kafka-Spark", test_kafka_spark_integration)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\n🧪 Test: {test_name}")
        print("-" * 40)
        results[test_name] = test_func()
        time.sleep(2)  # Pausa tra i test
    
    # Risultati finali
    print("\n" + "="*60)
    print("📊 RISULTATI TEST")
    print("="*60)
    
    all_passed = True
    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name:<30} {status}")
        if not result:
            all_passed = False
    
    print("-" * 60)
    if all_passed:
        print("🎉 TUTTI I TEST SUPERATI!")
        print("✅ La pipeline è pronta per il processing continuo")
        print("\n💡 Prossimi passi:")
        print("   1. Esegui: chmod +x scripts/submit_spark_job.sh")
        print("   2. Esegui: ./scripts/submit_spark_job.sh")
    else:
        print("⚠️  ALCUNI TEST FALLITI")
        print("🔧 Risolvi i problemi evidenziati prima di continuare")

if __name__ == "__main__":
    run_comprehensive_test()