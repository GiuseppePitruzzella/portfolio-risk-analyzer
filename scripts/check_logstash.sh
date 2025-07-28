#!/bin/bash

# Script di verifica dedicato per Logstash

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

echo "🔍 Portfolio Risk Analyzer - Verifica Logstash"
echo "=============================================="

# Test 1: Container Status
echo ""
echo "1️⃣  CONTAINER STATUS"
echo "==================="

if docker compose ps logstash | grep -q "Up"; then
    print_status "OK" "Container Logstash attivo"
    
    # Uptime
    uptime=$(docker compose ps logstash --format "{{.Status}}" | grep -o "Up [^(]*")
    print_status "INFO" "Status: $uptime"
else
    print_status "ERROR" "Container Logstash non attivo"
    echo "💡 Avvia con: docker compose up -d logstash"
    exit 1
fi

# Test 2: Health Endpoint
echo ""
echo "2️⃣  HEALTH ENDPOINT"
echo "=================="

if curl -s http://localhost:9600/_node/stats >/dev/null 2>&1; then
    print_status "OK" "Health endpoint raggiungibile"
    
    # Dettagli health
    health_response=$(curl -s http://localhost:9600/_node/stats)
    
    # Pipeline status
    if echo "$health_response" | grep -q '"pipeline"'; then
        print_status "OK" "Pipeline configurata"
        
        # Workers
        workers=$(echo "$health_response" | grep -o '"workers":[0-9]*' | cut -d: -f2 || echo "N/A")
        print_status "INFO" "Workers attivi: $workers"
        
        # Batch size
        batch_size=$(echo "$health_response" | grep -o '"batch_size":[0-9]*' | cut -d: -f2 || echo "N/A")
        print_status "INFO" "Batch size: $batch_size"
    else
        print_status "WARN" "Info pipeline non disponibili"
    fi
else
    print_status "ERROR" "Health endpoint non raggiungibile"
    echo "💡 Logstash potrebbe essere in fase di startup"
fi

# Test 3: Configurazione
echo ""
echo "3️⃣  CONFIGURAZIONE"
echo "=================="

# File configurazione
if docker compose exec -T logstash test -f /usr/share/logstash/pipeline/logstash.conf 2>/dev/null; then
    print_status "OK" "File configurazione presente"
    
    # Dimensione file
    config_size=$(docker compose exec -T logstash wc -c < /usr/share/logstash/pipeline/logstash.conf 2>/dev/null || echo "0")
    config_lines=$(docker compose exec -T logstash wc -l < /usr/share/logstash/pipeline/logstash.conf 2>/dev/null || echo "0")
    
    if [ "$config_lines" -gt 20 ]; then
        print_status "OK" "Configurazione completa ($config_lines righe, $config_size bytes)"
    else
        print_status "WARN" "Configurazione sospettosamente corta ($config_lines righe)"
    fi
    
    # Mostra sezioni principali
    print_status "INFO" "Sezioni configurazione:"
    docker compose exec -T logstash grep -n -E "^(input|filter|output)" /usr/share/logstash/pipeline/logstash.conf 2>/dev/null | while read line; do
        echo "   📋 $line"
    done
else
    print_status "ERROR" "File configurazione mancante"
    exit 1
fi

# Test 4: Variabili Ambiente
echo ""
echo "4️⃣  VARIABILI AMBIENTE"
echo "====================="

# API Key
env_vars=$(docker compose exec -T logstash env 2>/dev/null | grep FINNHUB || echo "NOT_FOUND")
if [ "$env_vars" != "NOT_FOUND" ]; then
    print_status "OK" "Variabile FINNHUB_API_KEY presente"
    
    # Verifica lunghezza (senza mostrare la chiave)
    key_value=$(echo "$env_vars" | cut -d'=' -f2)
    key_length=${#key_value}
    
    if [ "$key_length" -gt 10 ]; then
        print_status "OK" "API Key sembra valida (lunghezza: $key_length)"
    else
        print_status "WARN" "API Key sospettosamente corta (lunghezza: $key_length)"
    fi
else
    print_status "ERROR" "Variabile FINNHUB_API_KEY mancante"
    echo "💡 Verifica file .env e docker-compose.yml"
fi

# Altre variabili Logstash
logstash_vars=$(docker compose exec -T logstash env 2>/dev/null | grep -E "^LS_|^LOGSTASH_" || echo "")
if [ -n "$logstash_vars" ]; then
    print_status "INFO" "Variabili Logstash:"
    echo "$logstash_vars" | while read var; do
        echo "   🔧 $var"
    done
fi

# Test 5: Log Analysis
echo ""
echo "5️⃣  ANALISI LOG"
echo "==============="

print_status "INFO" "Analisi ultimi 50 log entries..."

# Errori
error_count=$(docker compose logs --tail=50 logstash 2>/dev/null | grep -c "ERROR" || echo "0")
if [ "$error_count" -eq 0 ]; then
    print_status "OK" "Nessun errore nei log recenti"
else
    print_status "WARN" "$error_count errori trovati nei log recenti"
    
    # Mostra ultimi errori
    print_status "INFO" "Ultimi errori:"
    docker compose logs --tail=50 logstash 2>/dev/null | grep "ERROR" | tail -3 | while read line; do
        echo "   ❌ $(echo "$line" | cut -c1-100)..."
    done
fi

# Warning
warn_count=$(docker compose logs --tail=50 logstash 2>/dev/null | grep -c "WARN" || echo "0")
if [ "$warn_count" -eq 0 ]; then
    print_status "OK" "Nessun warning nei log recenti"
else
    print_status "INFO" "$warn_count warning nei log recenti"
fi

# Info attività
activity_patterns=("http_poller" "Polling" "request" "response")
for pattern in "${activity_patterns[@]}"; do
    count=$(docker compose logs --tail=50 logstash 2>/dev/null | grep -ci "$pattern" || echo "0")
    if [ "$count" -gt 0 ]; then
        print_status "OK" "Attività '$pattern' rilevata ($count occorrenze)"
    fi
done

# Test 6: Test API
echo ""
echo "6️⃣  TEST API CONNECTIVITY"
echo "========================="

# Estrai API key per test
api_key=$(docker compose exec -T logstash env 2>/dev/null | grep FINNHUB_API_KEY | cut -d'=' -f2 | tr -d '\r\n' || echo "")

if [ -n "$api_key" ]; then
    print_status "INFO" "Test connettività API Finnhub..."
    
    # Test API call
    api_response=$(curl -s --max-time 10 "https://finnhub.io/api/v1/quote?symbol=SPY&token=$api_key" 2>/dev/null || echo "")
    
    if echo "$api_response" | grep -q '"c":[0-9]'; then
        # Estrai prezzo
        price=$(echo "$api_response" | grep -o '"c":[0-9.]*' | cut -d: -f2)
        change=$(echo "$api_response" | grep -o '"dp":[0-9.-]*' | cut -d: -f2)
        
        print_status "OK" "API Finnhub risponde correttamente"
        print_status "INFO" "SPY: \$${price} (${change}%)"
    else
        print_status "ERROR" "API Finnhub non risponde correttamente"
        print_status "INFO" "Risposta: ${api_response:0:100}..."
        
        # Verifica rate limiting
        if echo "$api_response" | grep -q "limit"; then
            print_status "WARN" "Possibile rate limiting API"
        fi
    fi
else
    print_status "WARN" "Non posso testare API - chiave non estratta"
fi

# Test 7: Test di Output (monitoring)
echo ""
echo "7️⃣  MONITORING OUTPUT (30 secondi)"
echo "================================="

print_status "INFO" "Monitoraggio output Logstash per 30 secondi..."
print_status "INFO" "Cerco pattern di attività..."

# Monitora log per attività
timeout 30s docker compose logs -f logstash 2>/dev/null | while IFS= read -r line; do
    case "$line" in
        *"Starting pipeline"*)
            print_status "OK" "Pipeline avviata"
            ;;
        *"Polling"*|*"request"*)
            print_status "OK" "Polling API attivo"
            ;;
        *"response"*|*"current_price"*|*"symbol"*)
            print_status "OK" "Dati ricevuti dall'API"
            ;;
        *"kafka"*|*"Sent"*|*"bootstrap"*)
            print_status "OK" "Comunicazione con Kafka"
            ;;
        *"ERROR"*)
            print_status "ERROR" "Errore: $(echo "$line" | cut -c50-150)..."
            ;;
        *"WARN"*)
            print_status "WARN" "Warning: $(echo "$line" | cut -c50-150)..."
            ;;
    esac
done 2>/dev/null || print_status "INFO" "Timeout monitoraggio completato"

# RISULTATO FINALE
echo ""
echo "🎯 RISULTATO VERIFICA LOGSTASH"
echo "=============================="

# Conteggio problemi
error_indicators=0

# Verifica container
if ! docker compose ps logstash | grep -q "Up"; then
    ((error_indicators++))
fi

# Verifica health
if ! curl -s http://localhost:9600/_node/stats >/dev/null 2>&1; then
    ((error_indicators++))
fi

# Verifica API key
if ! docker compose exec -T logstash env 2>/dev/null | grep -q FINNHUB_API_KEY; then
    ((error_indicators++))
fi

# Verifica errori nei log
recent_errors=$(docker compose logs --tail=20 logstash 2>/dev/null | grep -c "ERROR" || echo "0")
if [ "$recent_errors" -gt 0 ]; then
    ((error_indicators++))
fi

# Risultato
if [ "$error_indicators" -eq 0 ]; then
    print_status "OK" "LOGSTASH COMPLETAMENTE FUNZIONANTE!"
    echo ""
    echo "🚀 Logstash è configurato correttamente e attivo"
    echo "✅ Può procedere con test Kafka e Spark"
    exit 0
elif [ "$error_indicators" -eq 1 ]; then
    print_status "WARN" "LOGSTASH QUASI FUNZIONANTE"
    echo ""
    echo "🔧 Un problema minore rilevato"
    echo "💡 Logstash probabilmente funziona ma controlla i dettagli sopra"
    exit 1
else
    print_status "ERROR" "LOGSTASH HA PROBLEMI"
    echo ""
    echo "🔧 $error_indicators problemi rilevati"
    echo "💡 Risolvi i problemi evidenziati prima di procedere"
    echo ""
    echo "🛠️  Azioni suggerite:"
    echo "   1. Verifica file .env con API key valida"
    echo "   2. docker compose restart logstash"
    echo "   3. Controlla configurazione in config/logstash.conf"
    exit 1
fi