#!/bin/bash

# Script per verificare che Spark sia pronto per il submit

echo "🔍 Verifica Readiness di Spark"
echo "==============================="

# Test 1: Verifica che i container Spark siano up
echo ""
echo "1. Stato container Spark:"
if docker compose ps spark-master | grep -q "Up"; then
    echo "   ✅ spark-master: Up"
else
    echo "   ❌ spark-master: Non attivo"
    exit 1
fi

if docker compose ps spark-worker | grep -q "Up"; then
    echo "   ✅ spark-worker: Up"
else
    echo "   ❌ spark-worker: Non attivo"
    exit 1
fi

# Test 2: Verifica UI Spark Master
echo ""
echo "2. Spark Master UI:"
if curl -s http://localhost:8081 > /dev/null; then
    echo "   ✅ UI raggiungibile su http://localhost:8081"
else
    echo "   ❌ UI non raggiungibile"
    exit 1
fi

# Test 3: Verifica worker registrati
echo ""
echo "3. Worker registrati:"
worker_info=$(curl -s http://localhost:8081/json 2>/dev/null || echo "{}")
if echo "$worker_info" | grep -q '"workers"'; then
    worker_count=$(echo "$worker_info" | grep -o '"workers":\[[^]]*\]' | grep -o '"id":' | wc -l)
    if [ "$worker_count" -gt 0 ]; then
        echo "   ✅ $worker_count worker(s) registrato/i"
    else
        echo "   ⚠️  Nessun worker registrato (potrebbero essere in fase di startup)"
    fi
else
    echo "   ⚠️  Impossibile determinare il numero di worker"
fi

# Test 4: Test connettività interna Docker (usando telnet invece di nc)
echo ""
echo "4. Connettività interna:"
if docker compose exec -T spark-master timeout 5 bash -c "echo '' | telnet spark-master 7077" 2>/dev/null | grep -q "Connected"; then
    echo "   ✅ Porta 7077 raggiungibile internamente"
elif docker compose exec -T spark-master bash -c "timeout 3 bash -c '</dev/tcp/spark-master/7077' 2>/dev/null"; then
    echo "   ✅ Porta 7077 raggiungibile internamente (bash)"
else
    echo "   ⚠️  Test connettività non conclusivo, procediamo comunque"
    echo "   💡 La connettività verrà testata nel passo successivo"
fi

# Test 5: Test Spark submit semplice
echo ""
echo "5. Test Spark submit semplice:"

cat > /tmp/spark_readiness_test.py << 'EOF'
from pyspark.sql import SparkSession
import sys

try:
    spark = SparkSession.builder \
        .appName("Readiness Test") \
        .getOrCreate()
    
    df = spark.range(3)
    count = df.count()
    
    print(f"SUCCESS: Processed {count} rows")
    spark.stop()
    
except Exception as e:
    print(f"ERROR: {str(e)}")
    sys.exit(1)
EOF

# Copia e esegui il test
docker compose cp /tmp/spark_readiness_test.py spark-master:/tmp/spark_readiness_test.py

if docker compose exec -T spark-master spark-submit \
    --master spark://spark-master:7077 \
    /tmp/spark_readiness_test.py 2>&1 | grep -q "SUCCESS:"; then
    echo "   ✅ Test submit completato con successo"
else
    echo "   ❌ Test submit fallito"
    echo "   📋 Log per debug:"
    docker compose logs --tail=10 spark-master
    exit 1
fi

# Cleanup
rm -f /tmp/spark_readiness_test.py
docker compose exec -T spark-master rm -f /tmp/spark_readiness_test.py

echo ""
echo "🎉 SPARK È PRONTO!"
echo "==================="
echo "✅ Tutti i test di readiness superati"
echo "🚀 Puoi ora lanciare: ./scripts/submit_spark_job.sh"