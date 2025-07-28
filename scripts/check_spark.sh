#!/bin/bash

# Script per verificare il formato dei messaggi lungo tutta la pipeline

set -e

# Colori
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_status() {
    local status=$1
    local message=$2
    case $status in
        "OK") echo -e "${GREEN}✅ $message${NC}" ;;
        "WARN") echo -e "${YELLOW}⚠️  $message${NC}" ;;
        "ERROR") echo -e "${RED}❌ $message${NC}" ;;
        "INFO") echo -e "${BLUE}ℹ️  $message${NC}" ;;
    esac
}

validate_json() {
    local json_string="$1"
    local source="$2"
    
    # Test se è JSON valido
    if echo "$json_string" | jq . >/dev/null 2>&1; then
        print_status "OK" "$source: JSON valido"
        return 0
    else
        print_status "ERROR" "$source: JSON malformato"
        return 1
    fi
}

analyze_message_structure() {
    local json_string="$1"
    local source="$2"
    
    print_status "INFO" "Analisi struttura messaggio da $source:"
    
    # Campi richiesti per ETF
    required_fields=("symbol" "current_price" "timestamp")
    optional_fields=("change" "percent_change" "high" "low" "open" "previous_close")
    
    for field in "${required_fields[@]}"; do
        if echo "$json_string" | jq -e ".$field" >/dev/null 2>&1; then
            value=$(echo "$json_string" | jq -r ".$field" 2>/dev/null || echo "ERROR")
            print_status "OK" "   ✓ $field: $value"
        else
            print_status "ERROR" "   ✗ Campo richiesto mancante: $field"
        fi
    done
    
    for field in "${optional_fields[@]}"; do
        if echo "$json_string" | jq -e ".$field" >/dev/null 2>&1; then
            value=$(echo "$json_string" | jq -r ".$field" 2>/dev/null || echo "ERROR")
            print_status "INFO" "   + $field: $value"
        fi
    done
    
    # Verifica tipi di dati
    print_status "INFO" "Verifica tipi di dati:"
    
    # Symbol deve essere stringa
    symbol_type=$(echo "$json_string" | jq -r 'type' 2>/dev/null || echo "null")
    if echo "$json_string" | jq -e '.symbol | type == "string"' >/dev/null 2>&1; then
        print_status "OK" "   symbol: string ✓"
    else
        print_status "WARN" "   symbol: tipo non corretto"
    fi
    
    # Current_price deve essere numero
    if echo "$json_string" | jq -e '.current_price | type == "number"' >/dev/null 2>&1; then
        price=$(echo "$json_string" | jq -r '.current_price')
        if (( $(echo "$price > 0" | bc -l) )); then
            print_status "OK" "   current_price: numero positivo ✓"
        else
            print_status "WARN" "   current_price: numero ma non positivo"
        fi
    else
        print_status "ERROR" "   current_price: non è un numero"
    fi
    
    # Timestamp format check
    if echo "$json_string" | jq -e '.timestamp' >/dev/null 2>&1; then
        timestamp=$(echo "$json_string" | jq -r '.timestamp')
        if [[ "$timestamp" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ]]; then
            print_status "OK" "   timestamp: formato ISO ✓"
        else
            print_status "WARN" "   timestamp: formato non standard"
        fi
    fi
}

echo "🔍 Portfolio Risk Analyzer - Verifica Formato Messaggi"
echo "======================================================"

# Test 1: Verifica messaggi grezzi da API Finnhub
echo ""
echo "1️⃣  TEST API FINNHUB GREZZA"
echo "=========================="

# Estrai API key
api_key=$(docker compose exec -T logstash env 2>/dev/null | grep FINNHUB_API_KEY | cut -d'=' -f2 | tr -d '\r\n' || echo "")

if [ -n "$api_key" ]; then
    print_status "INFO" "Test risposta diretta API Finnhub..."
    
    api_response=$(curl -s --max-time 10 "https://finnhub.io/api/v1/quote?symbol=SPY&token=$api_key" 2>/dev/null || echo "")
    
    if [ -n "$api_response" ]; then
        echo ""
        echo "📥 Risposta grezza API:"
        echo "$api_response" | jq . 2>/dev/null || echo "$api_response"
        
        if validate_json "$api_response" "API Finnhub"; then
            print_status "INFO" "Analisi campi API Finnhub:"
            echo "$api_response" | jq -r 'to_entries[] | "   \(.key): \(.value)"' 2>/dev/null
        fi
    else
        print_status "ERROR" "Nessuna risposta dall'API Finnhub"
    fi
else
    print_status "WARN" "API key non disponibile per test diretto"
fi

# Test 2: Verifica messaggi in Kafka
echo ""
echo "2️⃣  TEST MESSAGGI KAFKA"
echo "======================"

print_status "INFO" "Lettura messaggi da topic Kafka..."

# Leggi alcuni messaggi da Kafka
kafka_messages=$(timeout 10s docker compose exec -T kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic financial_prices \
    --from-beginning \
    --max-messages 3 2>/dev/null || echo "")

if [ -n "$kafka_messages" ]; then
    message_count=$(echo "$kafka_messages" | wc -l)
    print_status "OK" "$message_count messaggi trovati in Kafka"
    
    echo ""
    message_num=1
    echo "$kafka_messages" | while read -r message; do
        if [ -n "$message" ]; then
            echo "📨 MESSAGGIO KAFKA #$message_num:"
            echo "Raw: $message"
            echo ""
            
            if validate_json "$message" "Kafka"; then
                analyze_message_structure "$message" "Kafka"
            fi
            echo ""
            ((message_num++))
        fi
    done
else
    print_status "WARN" "Nessun messaggio trovato in Kafka"
    print_status "INFO" "Questo è normale se Logstash non sta inviando dati"
fi

# Test 3: Verifica schema Spark
echo ""
echo "3️⃣  TEST SCHEMA SPARK"
echo "==================="

print_status "INFO" "Verifica schema atteso da Spark..."

# Mostra schema definito nel codice Spark
expected_schema='{
  "symbol": "string",
  "current_price": "double", 
  "change": "double",
  "percent_change": "double",
  "high": "double",
  "low": "double", 
  "open": "double",
  "previous_close": "double",
  "intraday_volatility": "double",
  "intraday_range": "double",
  "timestamp": "string",
  "data_source": "string",
  "data_type": "string",
  "doc_id": "string"
}'

echo "📋 Schema atteso da Spark:"
echo "$expected_schema" | jq .

# Test 4: Verifica con Spark in tempo reale
echo ""
echo "4️⃣  TEST PARSING SPARK"
echo "====================="

# Crea script Spark per testare il parsing
cat > /tmp/test_spark_parsing.py << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# Schema atteso
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("change", DoubleType(), True),
    StructField("percent_change", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("open", DoubleType(), True),
    StructField("previous_close", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("data_source", StringType(), True)
])

try:
    spark = SparkSession.builder.appName("Message Format Test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    print("SUCCESS: Spark session creata")
    
    # Test lettura da Kafka con schema
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "financial_prices") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    message_count = df.count()
    print(f"SUCCESS: {message_count} messaggi letti da Kafka")
    
    if message_count > 0:
        # Parse JSON
        parsed_df = df.select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("kafka_key", "data.*")
        
        # Verifica parsing
        valid_messages = parsed_df.filter(col("current_price").isNotNull() & col("symbol").isNotNull())
        valid_count = valid_messages.count()
        
        print(f"SUCCESS: {valid_count}/{message_count} messaggi parsati correttamente")
        
        if valid_count > 0:
            # Mostra campioni
            print("SUCCESS: Campioni messaggi parsati:")
            valid_messages.select("symbol", "current_price", "timestamp").show(3, truncate=False)
            
            # Verifica tipi di dati
            print("SUCCESS: Schema parsato:")
            valid_messages.printSchema()
        else:
            print("WARNING: Nessun messaggio valido dopo parsing")
    else:
        print("WARNING: Nessun messaggio in Kafka da testare")
    
    spark.stop()
    print("SUCCESS: Test completato")
    
except Exception as e:
    print(f"ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
EOF

if docker compose cp /tmp/test_spark_parsing.py spark-master:/tmp/test_spark_parsing.py 2>/dev/null; then
    print_status "INFO" "Esecuzione test parsing Spark..."
    
    spark_output=$(timeout 60s docker compose exec -T spark-master spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        --conf spark.executor.memory=512m \
        --conf spark.driver.memory=512m \
        /tmp/test_spark_parsing.py 2>&1 || echo "TIMEOUT_OR_ERROR")
    
    echo ""
    echo "📊 OUTPUT SPARK TEST:"
    echo "$spark_output"
    
    # Analizza risultati
    if echo "$spark_output" | grep -q "SUCCESS: Test completato"; then
        print_status "OK" "Test parsing Spark completato con successo"
        
        # Estrai statistiche
        if echo "$spark_output" | grep -q "messaggi parsati correttamente"; then
            stats=$(echo "$spark_output" | grep "messaggi parsati correttamente")
            print_status "OK" "$stats"
        fi
    else
        print_status "WARN" "Test parsing Spark con problemi"
    fi
    
    # Cleanup
    rm -f /tmp/test_spark_parsing.py
    docker compose exec -T spark-master rm -f /tmp/test_spark_parsing.py 2>/dev/null || true
else
    print_status "ERROR" "Impossibile copiare script test nel container Spark"
fi

# Test 5: Verifica messaggi con jq più approfondita
echo ""
echo "5️⃣  ANALISI APPROFONDITA MESSAGGI"
echo "================================"

if [ -n "$kafka_messages" ]; then
    print_status "INFO" "Analisi avanzata struttura messaggi..."
    
    # Prendi il primo messaggio per analisi dettagliata
    first_message=$(echo "$kafka_messages" | head -1)
    
    if [ -n "$first_message" ]; then
        echo ""
        echo "🔬 ANALISI DETTAGLIATA PRIMO MESSAGGIO:"
        echo "======================================="
        
        # Struttura completa
        echo "📋 Struttura completa:"
        echo "$first_message" | jq . 2>/dev/null || echo "JSON malformato"
        
        # Tutti i campi e tipi
        echo ""
        echo "📊 Tutti i campi con tipi:"
        echo "$first_message" | jq -r 'to_entries[] | "\(.key): \(.value) (tipo: \(.value | type))"' 2>/dev/null || echo "Errore parsing"
        
        # Dimensione messaggio
        message_size=$(echo "$first_message" | wc -c)
        print_status "INFO" "Dimensione messaggio: $message_size bytes"
        
        # Valida ogni campo critico
        echo ""
        echo "🎯 VALIDAZIONE CAMPI CRITICI:"
        
        # Symbol validation
        symbol=$(echo "$first_message" | jq -r '.symbol // empty' 2>/dev/null)
        if [[ "$symbol" =~ ^(SPY|QQQ|IWM)$ ]]; then
            print_status "OK" "Symbol: '$symbol' ✓"
        else
            print_status "WARN" "Symbol: '$symbol' - non nell'elenco atteso"
        fi
        
        # Price validation  
        price=$(echo "$first_message" | jq -r '.current_price // empty' 2>/dev/null)
        if [[ "$price" =~ ^[0-9]+\.?[0-9]*$ ]] && (( $(echo "$price > 0" | bc -l) )); then
            print_status "OK" "Current Price: $price ✓"
        else
            print_status "ERROR" "Current Price: '$price' - non valido"
        fi
        
        # Timestamp validation
        timestamp=$(echo "$first_message" | jq -r '.timestamp // empty' 2>/dev/null)
        if [ -n "$timestamp" ]; then
            print_status "OK" "Timestamp: $timestamp ✓"
        else
            print_status "WARN" "Timestamp: mancante o vuoto"
        fi
    fi
fi

# RISULTATO FINALE
echo ""
echo "🎯 RISULTATO VERIFICA FORMATO MESSAGGI"
echo "======================================"

# Conteggio problemi
format_issues=0

# Verifica se abbiamo messaggi
if [ -z "$kafka_messages" ]; then
    print_status "WARN" "NESSUN MESSAGGIO DA VERIFICARE"
    echo ""
    echo "🔧 Possibili cause:"
    echo "   - Logstash non sta inviando dati"
    echo "   - Topic Kafka vuoto"
    echo "   - Problemi di connettività"
    exit 1
fi

# Analisi qualità messaggi
valid_json=true
valid_structure=true

echo "$kafka_messages" | while read -r message; do
    if [ -n "$message" ]; then
        if ! echo "$message" | jq . >/dev/null 2>&1; then
            valid_json=false
        fi
        
        if ! echo "$message" | jq -e '.symbol and .current_price' >/dev/null 2>&1; then
            valid_structure=false
        fi
    fi
done

if [ "$valid_json" = true ] && [ "$valid_structure" = true ]; then
    print_status "OK" "FORMATO MESSAGGI CORRETTO!"
    echo ""
    echo "✅ Messaggi JSON validi"
    echo "✅ Struttura dati corretta" 
    echo "✅ Campi richiesti presenti"
    echo "✅ Spark può processare i dati"
    echo ""
    echo "🚀 PIPELINE PRONTA PER:"
    echo "   ✅ Stream processing Spark"
    echo "   ✅ Calcoli metriche di rischio"
    echo "   ✅ Salvataggio Elasticsearch"
else
    print_status "ERROR" "PROBLEMI NEL FORMATO MESSAGGI"
    echo ""
    echo "🔧 Azioni necessarie:"
    echo "   1. Verifica configurazione Logstash"
    echo "   2. Controlla filtri di parsing"
    echo "   3. Testa API Finnhub direttamente"
    echo "   4. Considera manual data feeder per bypass"
fi