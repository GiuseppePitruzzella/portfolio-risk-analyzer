#!/bin/bash

# Submit Spark Job Script per Portfolio Risk Analyzer
# Questo script facilita il lancio del job Spark nel cluster

set -e

echo "🚀 Portfolio Risk Analyzer - Spark Job Submitter"
echo "================================================"

# Configurazioni
SPARK_MASTER="spark://spark-master:7077"  # Usa hostname interno Docker
APP_NAME="Portfolio Risk Analyzer"
MAIN_FILE="/opt/bitnami/spark/jobs/stream_processor.py"
DRIVER_MEMORY="1g"
EXECUTOR_MEMORY="1g"
EXECUTOR_CORES="1"

# Verifica che Spark sia in esecuzione
echo "🔍 Verifica stato Spark Master..."
if ! curl -s http://localhost:8081 > /dev/null; then
    echo "❌ Spark Master non raggiungibile su http://localhost:8081"
    echo "💡 Assicurati che Docker Compose sia in esecuzione:"
    echo "   docker compose up -d spark-master spark-worker"
    exit 1
fi

echo "✅ Spark Master raggiungibile"

# Verifica che il file Python esista
if [ ! -f "scripts/spark/stream_processor.py" ]; then
    echo "❌ File stream_processor.py non trovato in scripts/spark/"
    echo "💡 Assicurati che il file sia presente e che tu sia nella directory corretta"
    exit 1
fi

echo "✅ Script Spark trovato"

# Verifica che Kafka sia in esecuzione
echo "🔍 Verifica stato Kafka..."
if ! docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; then
    echo "❌ Kafka non raggiungibile"
    echo "💡 Assicurati che Kafka sia in esecuzione:"
    echo "   docker compose up -d kafka"
    exit 1
fi

echo "✅ Kafka raggiungibile"

# Lista i topic Kafka per debug
echo "📋 Topic Kafka disponibili:"
docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list | sed 's/^/   /'

# Verifica che il file Python esista
if [ ! -f "scripts/spark/stream_processor.py" ]; then
    echo "❌ File stream_processor.py non trovato in scripts/spark/"
    echo "💡 Assicurati che il file sia presente e che tu sia nella directory corretta"
    exit 1
fi

echo "✅ Script Spark trovato"

# Submit del job
echo "🚀 Lancio del job Spark..."
echo "📊 Parametri:"
echo "   - Master: $SPARK_MASTER"
echo "   - Driver Memory: $DRIVER_MEMORY"
echo "   - Executor Memory: $EXECUTOR_MEMORY"
echo "   - Executor Cores: $EXECUTOR_CORES"
echo ""

docker compose exec spark-master spark-submit \
    --master "$SPARK_MASTER" \
    --name "$APP_NAME" \
    --driver-memory "$DRIVER_MEMORY" \
    --executor-memory "$EXECUTOR_MEMORY" \
    --executor-cores "$EXECUTOR_CORES" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    "$MAIN_FILE"

echo ""
echo "🎯 Job completato!"
echo "📊 Per monitorare i job Spark:"
echo "   - Spark Master UI: http://localhost:8081"
echo "   - Spark Worker UI: http://localhost:8082"
echo "   - Kafka UI: http://localhost:8080"