#!/bin/bash

# Submit Spark Job Diretto (senza pre-check problematici)

set -e

echo "🚀 Portfolio Risk Analyzer - Submit Diretto"
echo "============================================"

# Configurazioni
SPARK_MASTER="spark://spark-master:7077"
APP_NAME="Portfolio Risk Analyzer"
MAIN_FILE="/opt/bitnami/spark/jobs/stream_processor.py"
DRIVER_MEMORY="1g"
EXECUTOR_MEMORY="1g"
EXECUTOR_CORES="1"

# Solo verifiche essenziali
echo "🔍 Verifiche preliminari..."

# Verifica container up
if ! docker compose ps spark-master | grep -q "Up"; then
    echo "❌ Container spark-master non è up"
    exit 1
fi

# Verifica file esista
if [ ! -f "scripts/spark/stream_processor.py" ]; then
    echo "❌ File stream_processor.py non trovato"
    exit 1
fi

echo "✅ Verifiche OK"

# Informazioni
echo ""
echo "📊 Configurazione job:"
echo "   - Master: $SPARK_MASTER"
echo "   - App: $APP_NAME"
echo "   - Driver Memory: $DRIVER_MEMORY"
echo "   - Executor Memory: $EXECUTOR_MEMORY"
echo ""
echo "💡 Il job si connetterà direttamente al cluster"
echo "⏱️  Timeout: 2 minuti per l'avvio"
echo "🛑 Premi Ctrl+C per fermare"
echo ""

# Submit con timeout
echo "🚀 Avvio job Spark..."

timeout 120s docker compose exec spark-master spark-submit \
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
    --conf spark.executor.extraJavaOptions="-XX:+UseG1GC" \
    --conf spark.driver.extraJavaOptions="-XX:+UseG1GC" \
    --conf spark.network.timeout=300s \
    --conf spark.executor.heartbeatInterval=60s \
    "$MAIN_FILE" || {
    
    exit_code=$?
    echo ""
    echo "⚠️  Job terminato con codice $exit_code"
    
    if [ $exit_code -eq 124 ]; then
        echo "⏰ Timeout di 2 minuti raggiunto"
        echo "💡 Possibili cause:"
        echo "   - Worker non disponibile"
        echo "   - Download dipendenze lento"
        echo "   - Problemi di connettività"
    else
        echo "❌ Errore durante l'esecuzione"
        echo "📋 Controlla i log per dettagli:"
        echo "   docker compose logs spark-master | tail -20"
        echo "   docker compose logs spark-worker | tail -20"
    fi
    
    exit $exit_code
}

echo ""
echo "🎯 Job completato!"