#!/bin/bash

# Script per eseguire l'analisi delle metriche di rischio

echo "🧮 Portfolio Risk Analyzer - Analisi Metriche di Rischio"
echo "========================================================"

# Verifica che Elasticsearch sia up
echo "🔍 Verifica Elasticsearch..."
if ! curl -s http://localhost:9200/_cluster/health | grep -q "yellow\|green"; then
    echo "❌ Elasticsearch non è raggiungibile o non è healthy"
    echo "💡 Avvia con: docker compose up -d elasticsearch"
    exit 1
fi

echo "✅ Elasticsearch raggiungibile"

# Verifica che ci siano dati
echo "🔍 Verifica presenza dati storici..."
data_count=$(curl -s "http://localhost:9200/etf-prices-*/_count" | grep -o '"count":[0-9]*' | cut -d: -f2 || echo "0")

if [ "$data_count" -eq 0 ]; then
    echo "⚠️  Nessun dato storico trovato in Elasticsearch"
    echo "💡 Assicurati che lo stream processor stia salvando dati:"
    echo "   ./scripts/submit_spark_job.sh"
    echo "   Aspetta almeno 5-10 minuti per accumulare dati storici"
    exit 1
fi

echo "✅ Trovati $data_count record storici"

# Crea directory per i risultati se non esiste
mkdir -p data

# Verifica file script
if [ ! -f "scripts/risk_metrics_calculator.py" ]; then
    echo "❌ File risk_metrics_calculator.py non trovato"
    exit 1
fi

# Copia nel container Spark
echo "📋 Preparazione script..."
cp scripts/risk_metrics_calculator.py scripts/spark/

# Esegui l'analisi
echo "🚀 Avvio analisi metriche di rischio..."
echo "⏱️  Questo potrebbe richiedere alcuni minuti..."
echo ""

docker compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --name "Portfolio Risk Analyzer - Risk Metrics" \
    --driver-memory 1g \
    --executor-memory 1g \
    --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    /opt/bitnami/spark/jobs/risk_metrics_calculator.py

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo ""
    echo "🎉 ANALISI COMPLETATA CON SUCCESSO!"
    echo "=================================="
    echo "📊 Le metriche di rischio sono state calcolate e salvate in:"
    echo "   - Elasticsearch (indice: risk-metrics-*)"
    echo "   - File JSON locale (directory: data/)"
    echo ""
    echo "🔍 Per visualizzare i risultati:"
    echo "   - Kibana: http://localhost:5601"
    echo "   - Query diretta Elasticsearch:"
    echo "     curl -s 'http://localhost:9200/risk-metrics-*/_search?pretty'"
    echo ""
    echo "📈 Metriche calcolate:"
    echo "   ✅ Value at Risk (VaR) al 95% e 99%"
    echo "   ✅ Conditional VaR (CVaR) al 95% e 99%"
    echo "   ✅ Tracking Error vs SPY"
    echo "   ✅ Matrice di Correlazione tra ETF"
else
    echo ""
    echo "❌ ERRORE NELL'ANALISI"
    echo "====================="
    echo "Exit code: $exit_code"
    echo ""
    echo "🔧 Possibili soluzioni:"
    echo "   1. Verifica che ci siano abbastanza dati storici"
    echo "   2. Controlla i log Spark:"
    echo "      docker compose logs spark-master | tail -20"
    echo "   3. Verifica connettività Elasticsearch:"
    echo "      curl http://localhost:9200/_cluster/health"
fi

echo ""
echo "📊 Per eseguire nuovamente l'analisi:"
echo "   ./scripts/run_risk_analysis.sh"