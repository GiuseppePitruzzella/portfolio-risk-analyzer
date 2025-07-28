#!/bin/bash

# Script di verifica dedicato per Kafka

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

echo "🔍 Portfolio Risk Analyzer - Verifica Kafka"
echo "==========================================="

# Test 1: Container Status
echo ""
echo "1️⃣  CONTAINER STATUS"
echo "==================="

# Kafka
if docker compose ps kafka | grep -q "Up"; then
    print_status "OK" "Container Kafka attivo"
    
    kafka_status=$(docker compose ps kafka --format "{{.Status}}")
    print_status "INFO" "Status: $kafka_status"
else
    print_status "ERROR" "Container Kafka non attivo"
    echo "💡 Avvia con: docker compose up -d kafka"
    exit 1
fi

# Zookeeper
if docker compose ps zookeeper | grep -q "Up"; then
    print_status "OK" "Container Zookeeper attivo"
else
    print_status "WARN" "Container Zookeeper non attivo"
    echo "💡 Kafka potrebbe non funzionare senza Zookeeper"
fi

# Test 2: Network Connectivity
echo ""
echo "2️⃣  CONNETTIVITÀ RETE"
echo "====================="

# Test porta Kafka
if nc -z localhost 9092 2>/dev/null; then
    print_status "OK" "Porta 9092 raggiungibile dall'host"
else
    print_status "ERROR" "Porta 9092 non raggiungibile"
fi

# Test connettività interna
if docker compose exec -T kafka true 2>/dev/null; then
    print_status "OK" "Container Kafka accessibile"
    
    # Test connettività interna
    if docker compose exec -T kafka timeout 5 bash -c 'echo > /dev/tcp/localhost/9092' 2>/dev/null; then
        print_status "OK" "Kafka broker interno raggiungibile"
    else
        print_status "WARN" "Test connettività interna inconclusivo"
    fi
else
    print_status "ERROR" "Container Kafka non accessibile"
    exit 1
fi

# Test 3: Kafka Broker Status
echo ""
echo "3️⃣  KAFKA BROKER STATUS"
echo "======================="

# Test basic connectivity
if docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    print_status "OK" "Kafka broker risponde ai comandi"
else
    print_status "ERROR" "Kafka broker non risponde"
    echo "💡 Kafka potrebbe essere in fase di startup"
    exit 1
fi

# Broker info
broker_info=$(docker compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 2>/dev/null | head -1 || echo "N/A")
if [ "$broker_info" != "N/A" ]; then
    print_status "INFO" "Broker info: $broker_info"
fi

# Cluster info
cluster_id=$(docker compose exec -T kafka kafka-cluster --bootstrap-server localhost:9092 cluster-id 2>/dev/null || echo "N/A")
if [ "$cluster_id" != "N/A" ]; then
    print_status "INFO" "Cluster ID: $cluster_id"
fi

# Test 4: Topic Management
echo ""
echo "4️⃣  GESTIONE TOPIC"
echo "=================="

# Lista topic esistenti
print_status "INFO" "Topic esistenti:"
topics=$(docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null || echo "ERROR")

if [ "$topics" != "ERROR" ]; then
    if [ -n "$topics" ]; then
        echo "$topics" | while read topic; do
            if [ -n "$topic" ]; then
                echo "   📋 $topic"
            fi
        done
        
        topic_count=$(echo "$topics" | wc -l)
        print_status "OK" "$topic_count topic trovati"
    else
        print_status "WARN" "Nessun topic trovato"
    fi
else
    print_status "ERROR" "Impossibile elencare topic"
fi

# Verifica topic financial_prices
if echo "$topics" | grep -q "financial_prices"; then
    print_status "OK" "Topic 'financial_prices' esiste"
    
    # Dettagli topic
    topic_details=$(docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic financial_prices 2>/dev/null)
    
    if [ -n "$topic_details" ]; then
        partitions=$(echo "$topic_details" | grep -o "PartitionCount:[0-9]*" | cut -d: -f2)
        replication=$(echo "$topic_details" | grep -o "ReplicationFactor:[0-9]*" | cut -d: -f2)
        
        print_status "INFO" "Partizioni: $partitions, Replication Factor: $replication"
        
        # Mostra configurazione partizioni
        print_status "INFO" "Configurazione partizioni:"
        echo "$topic_details" | grep "Partition:" | while read line; do
            echo "   🔧 $line"
        done
    fi
else
    print_status "WARN" "Topic 'financial_prices' non esiste"
    print_status "INFO" "Verrà creato automaticamente al primo messaggio"
fi

# Test 5: Message Analysis
echo ""
echo "5️⃣  ANALISI MESSAGGI"
echo "==================="

# Conta messaggi nel topic financial_prices
if echo "$topics" | grep -q "financial_prices"; then
    message_count=$(docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 \
        --topic financial_prices \
        --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum+0}' || echo "0")
    
    if [ "$message_count" -gt 0 ]; then
        print_status "OK" "$message_count messaggi nel topic financial_prices"
        
        # Analizza messaggi recenti
        print_status "INFO" "Campioni messaggi recenti:"
        timeout 10s docker compose exec -T kafka kafka-console-consumer \
            --bootstrap-server localhost:9092 \
            --topic financial_prices \
            --from-beginning \
            --max-messages 3 2>/dev/null | while read message; do
                # Estrai info essenziali dal JSON
                symbol=$(echo "$message" | grep -o '"symbol":"[^"]*"' | cut -d'"' -f4 2>/dev/null || echo "N/A")
                price=$(echo "$message" | grep -o '"current_price":[0-9.]*' | cut -d: -f2 2>/dev/null || echo "N/A")
                timestamp=$(echo "$message" | grep -o '"timestamp":"[^"]*"' | cut -d'"' -f4 2>/dev/null || echo "N/A")
                
                echo "   📊 $symbol: \$$price @ $timestamp"
            done || print_status "WARN" "Timeout lettura messaggi"
    else
        print_status "WARN" "Nessun messaggio nel topic financial_prices"
        print_status "INFO" "Topic vuoto - normale se Logstash non sta inviando dati"
    fi
else
    print_status "INFO" "Topic financial_prices non esiste - normale se non ancora creato"
fi

# Test 6: Performance Test
echo ""
echo "6️⃣  TEST PERFORMANCE"
echo "==================="

print_status "INFO" "Test invio messaggio di prova..."

# Test producer
test_message='{"test":"kafka_verification","timestamp":"'$(date -Iseconds)'","symbol":"TEST"}'

if echo "$test_message" | docker compose exec -T -i kafka kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic financial_prices 2>/dev/null; then
    print_status "OK" "Messaggio di test inviato con successo"
    
    # Verifica ricezione
    sleep 2
    received_test=$(timeout 5s docker compose exec -T kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic financial_prices \
        --from-beginning 2>/dev/null | grep "kafka_verification" | head -1 || echo "")
    
    if [ -n "$received_test" ]; then
        print_status "OK" "Messaggio di test ricevuto correttamente"
    else
        print_status "WARN" "Messaggio di test non trovato (potrebbe essere nel mezzo di molti altri)"
    fi
else
    print_status "ERROR" "Impossibile inviare messaggio di test"
fi

# Test 7: Consumer Group
echo ""
echo "7️⃣  CONSUMER GROUPS"
echo "=================="

# Lista consumer groups
consumer_groups=$(docker compose exec -T kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list 2>/dev/null || echo "")

if [ -n "$consumer_groups" ]; then
    print_status "OK" "Consumer groups attivi:"
    echo "$consumer_groups" | while read group; do
        if [ -n "$group" ]; then
            echo "   👥 $group"
        fi
    done
else
    print_status "INFO" "Nessun consumer group attivo"
fi

# Test 8: Log Analysis
echo ""
echo "8️⃣  ANALISI LOG KAFKA"
echo "=====================" 

# Analizza log Kafka
error_count=$(docker compose logs --tail=50 kafka 2>/dev/null | grep -c "ERROR" || echo "0")
if [ "$error_count" -eq 0 ]; then
    print_status "OK" "Nessun errore nei log Kafka"
else
    print_status "WARN" "$error_count errori nei log Kafka"
    
    # Mostra errori recenti
    print_status "INFO" "Errori recenti:"
    docker compose logs --tail=20 kafka 2>/dev/null | grep "ERROR" | tail -2 | while read line; do
        echo "   ❌ $(echo "$line" | cut -c50-120)..."
    done
fi

# Verifica startup completato
if docker compose logs --tail=20 kafka 2>/dev/null | grep -q "started"; then
    print_status "OK" "Kafka startup completato"
fi

# RISULTATO FINALE
echo ""
echo "🎯 RISULTATO VERIFICA KAFKA"
echo "==========================="

# Conteggio problemi
error_indicators=0

# Verifica container
if ! docker compose ps kafka | grep -q "Up"; then
    ((error_indicators++))
fi

# Verifica connettività
if ! nc -z localhost 9092 2>/dev/null; then
    ((error_indicators++))
fi

# Verifica broker
if ! docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    ((error_indicators++))
fi

# Verifica errori nei log
if [ "$error_count" -gt 2 ]; then
    ((error_indicators++))
fi

# Risultato
current_message_count=$(docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic financial_prices \
    --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum+0}' || echo "0")

if [ "$error_indicators" -eq 0 ]; then
    print_status "OK" "KAFKA COMPLETAMENTE FUNZIONANTE!"
    echo ""
    echo "🚀 Kafka è configurato correttamente e operativo"
    echo "📊 Messaggi attuali nel topic: $current_message_count"
    
    if [ "$current_message_count" -gt 0 ]; then
        echo "✅ Kafka sta ricevendo dati - pipeline funzionante"
    else
        echo "💡 Kafka pronto ma nessun messaggio - verifica Logstash"
    fi
    exit 0
elif [ "$error_indicators" -eq 1 ]; then
    print_status "WARN" "KAFKA QUASI FUNZIONANTE"
    echo ""
    echo "🔧 Un problema minore rilevato"
    echo "💡 Kafka probabilmente funziona ma controlla i dettagli sopra"
    exit 1
else
    print_status "ERROR" "KAFKA HA PROBLEMI"
    echo ""
    echo "🔧 $error_indicators problemi rilevati"
    echo "💡 Risolvi i problemi evidenziati prima di procedere"
    echo ""
    echo "🛠️  Azioni suggerite:"
    echo "   1. docker compose restart kafka zookeeper"
    echo "   2. Verifica porte non in conflitto: lsof -i :9092"
    echo "   3. Controlla log: docker compose logs kafka"
    exit 1
fi