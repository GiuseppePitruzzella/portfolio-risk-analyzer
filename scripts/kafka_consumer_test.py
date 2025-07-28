#!/usr/bin/env python3
"""
Kafka Consumer Test Script per Portfolio Risk Analyzer
Questo script legge i messaggi dal topic Kafka per verificare 
che Logstash stia inviando correttamente i dati dell'API Finnhub.
"""

import json
import os
import signal
import sys
from datetime import datetime
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Carica variabili d'ambiente
load_dotenv()

class ETFDataConsumer:
    def __init__(self):
        """Inizializza il consumer Kafka"""
        self.topic = os.getenv('KAFKA_TOPIC', 'financial_prices')
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.consumer = None
        self.message_count = 0
        self.running = True
        
        # Setup signal handler per graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, sig, frame):
        """Gestisce l'interruzione del programma"""
        print(f"\n🛑 Ricevuto segnale {sig}. Chiusura in corso...")
        self.running = False
        if self.consumer:
            self.consumer.close()
        sys.exit(0)
    
    def connect(self):
        """Connette al cluster Kafka"""
        try:
            print(f"🔌 Connessione a Kafka: {self.bootstrap_servers}")
            print(f"📺 Topic: {self.topic}")
            
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=[self.bootstrap_servers],
                # Inizia dal messaggio più recente
                auto_offset_reset='latest',
                # Disabilita auto-commit per testing
                enable_auto_commit=False,
                # Deserializza JSON automaticamente
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                # Timeout per il polling
                consumer_timeout_ms=1000,
                # ID del consumer group
                group_id='risk_analyzer_test_group'
            )
            
            print("✅ Connessione a Kafka riuscita!")
            return True
            
        except Exception as e:
            print(f"❌ Errore di connessione a Kafka: {e}")
            return False
    
    def format_message(self, message):
        """Formatta il messaggio per la visualizzazione"""
        try:
            data = message.value
            key = message.key
            partition = message.partition
            offset = message.offset
            timestamp = datetime.fromtimestamp(message.timestamp / 1000) if message.timestamp else "N/A"
            
            # Header informativo
            print("\n" + "="*80)
            print(f"📊 MESSAGGIO #{self.message_count + 1}")
            print(f"🔑 Key: {key}")
            print(f"📦 Partition: {partition} | Offset: {offset}")
            print(f"⏰ Timestamp: {timestamp}")
            print("-" * 80)
            
            # Dati ETF
            if isinstance(data, dict):
                symbol = data.get('symbol', 'N/A')
                current_price = data.get('current_price', 0)
                change = data.get('change', 0)
                percent_change = data.get('percent_change', 0)
                
                print(f"🏷️  Symbol: {symbol}")
                print(f"💰 Current Price: ${current_price:.2f}")
                print(f"📈 Change: ${change:+.2f} ({percent_change:+.2f}%)")
                
                if 'high' in data and 'low' in data:
                    print(f"📊 Day Range: ${data['low']:.2f} - ${data['high']:.2f}")
                
                if 'intraday_volatility' in data:
                    print(f"📉 Intraday Volatility: {data['intraday_volatility']:.2f}%")
                
                if 'open' in data:
                    print(f"🌅 Open: ${data['open']:.2f}")
                
                # Mostra tutti i campi per debug
                print("\n🔍 Dati completi:")
                for key, value in data.items():
                    if key not in ['symbol', 'current_price', 'change', 'percent_change', 'high', 'low', 'open', 'intraday_volatility']:
                        print(f"   {key}: {value}")
            else:
                print(f"📋 Raw Data: {data}")
            
            print("="*80)
            
        except Exception as e:
            print(f"❌ Errore nel formattare il messaggio: {e}")
            print(f"Raw message: {message}")
    
    def start_consuming(self):
        """Avvia il consumo dei messaggi"""
        if not self.connect():
            return
        
        print(f"\n🚀 Avvio consumer per topic '{self.topic}'...")
        print("💡 Premi Ctrl+C per fermare il consumer")
        print("⏳ In attesa di messaggi...\n")
        
        try:
            while self.running:
                # Poll per nuovi messaggi
                message_batch = self.consumer.poll(timeout_ms=1000, max_records=10)
                
                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            self.message_count += 1
                            self.format_message(message)
                            
                            # Commit manuale per essere sicuri
                            self.consumer.commit_async()
                else:
                    # Stampa un punto ogni 10 secondi se non ci sono messaggi
                    print(".", end="", flush=True)
                    
        except Exception as e:
            print(f"\n❌ Errore durante il consumo: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
            print(f"\n📊 Messaggi totali ricevuti: {self.message_count}")

def test_kafka_connection():
    """Testa la connessione a Kafka prima di avviare il consumer"""
    try:
        from kafka.admin import KafkaAdminClient
        admin_client = KafkaAdminClient(
            bootstrap_servers='localhost:9092',
            client_id='test_connection'
        )
        
        # Lista dei topic disponibili
        metadata = admin_client.describe_topics()
        print("🔍 Kafka è raggiungibile!")
        return True
        
    except Exception as e:
        print(f"❌ Kafka non è raggiungibile: {e}")
        print("💡 Assicurati che Docker Compose sia in esecuzione:")
        print("   docker compose up -d kafka")
        return False

if __name__ == "__main__":
    print("🚀 Portfolio Risk Analyzer - Kafka Consumer Test")
    print("="*60)
    
    # Test connessione
    if not test_kafka_connection():
        sys.exit(1)
    
    # Avvia consumer
    consumer = ETFDataConsumer()
    consumer.start_consuming()