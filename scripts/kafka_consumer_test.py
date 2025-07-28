#!/usr/bin/env python3
"""
Analizzatore dettagliato formato messaggi per Portfolio Risk Analyzer
Fornisce analisi approfondita della qualità e formato dei dati
"""

import json
import re
from datetime import datetime
from typing import Dict, List, Any, Optional
from kafka import KafkaConsumer
import requests

class MessageFormatAnalyzer:
    def __init__(self):
        self.kafka_servers = ['localhost:9092']
        self.topic = 'financial_prices'
        self.expected_schema = {
            'symbol': str,
            'current_price': (int, float),
            'change': (int, float), 
            'percent_change': (int, float),
            'high': (int, float),
            'low': (int, float),
            'open': (int, float),
            'timestamp': str,
            'data_source': str
        }
        
    def analyze_api_response(self, api_key: str) -> Dict:
        """Analizza risposta diretta API Finnhub"""
        print("🔍 Analisi risposta API Finnhub...")
        
        try:
            url = f"https://finnhub.io/api/v1/quote?symbol=SPY&token={api_key}"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                analysis = {
                    'status': 'OK',
                    'raw_data': data,
                    'fields': list(data.keys()),
                    'field_types': {k: type(v).__name__ for k, v in data.items()},
                    'field_count': len(data),
                    'data_size': len(json.dumps(data))
                }
                
                print(f"✅ API Response OK - {analysis['field_count']} campi, {analysis['data_size']} bytes")
                return analysis
            else:
                return {'status': 'ERROR', 'code': response.status_code, 'message': response.text}
                
        except Exception as e:
            return {'status': 'ERROR', 'exception': str(e)}
    
    def validate_message_structure(self, message: Dict) -> Dict:
        """Valida struttura di un singolo messaggio"""
        issues = []
        valid_fields = 0
        
        # Verifica campi richiesti
        for field, expected_type in self.expected_schema.items():
            if field in message:
                value = message[field]
                if isinstance(expected_type, tuple):
                    if any(isinstance(value, t) for t in expected_type):
                        valid_fields += 1
                    else:
                        issues.append(f"Campo '{field}': tipo {type(value).__name__}, atteso {expected_type}")
                else:
                    if isinstance(value, expected_type):
                        valid_fields += 1
                    else:
                        issues.append(f"Campo '{field}': tipo {type(value).__name__}, atteso {expected_type.__name__}")
            else:
                issues.append(f"Campo mancante: '{field}'")
        
        # Validazioni specifiche
        if 'symbol' in message:
            if message['symbol'] not in ['SPY', 'QQQ', 'IWM']:
                issues.append(f"Symbol '{message['symbol']}' non riconosciuto")
        
        if 'current_price' in message:
            price = message['current_price']
            if isinstance(price, (int, float)) and price <= 0:
                issues.append(f"Prezzo non valido: {price}")
        
        if 'timestamp' in message:
            timestamp = message['timestamp']
            # Verifica formato ISO
            iso_pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
            if not re.match(iso_pattern, str(timestamp)):
                issues.append(f"Timestamp formato non standard: {timestamp}")
        
        return {
            'valid_fields': valid_fields,
            'total_expected': len(self.expected_schema),
            'issues': issues,
            'is_valid': len(issues) == 0 and valid_fields >= 3  # Almeno 3 campi chiave
        }
    
    def analyze_kafka_messages(self, max_messages: int = 10) -> Dict:
        """Analizza messaggi da Kafka"""
        print(f"📨 Analisi messaggi Kafka (max {max_messages})...")
        
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.kafka_servers,
                auto_offset_reset='earliest',
                consumer_timeout_ms=15000,
                value_deserializer=lambda x: x.decode('utf-8') if x else None
            )
            
            messages = []
            for message in consumer:
                try:
                    # Parse JSON
                    data = json.loads(message.value)
                    
                    msg_analysis = {
                        'raw_message': message.value,
                        'parsed_data': data,
                        'key': message.key.decode('utf-8') if message.key else None,
                        'partition': message.partition,
                        'offset': message.offset,
                        'timestamp': message.timestamp,
                        'size_bytes': len(message.value),
                        'validation': self.validate_message_structure(data)
                    }
                    
                    messages.append(msg_analysis)
                    
                    if len(messages) >= max_messages:
                        break
                        
                except json.JSONDecodeError as e:
                    messages.append({
                        'raw_message': message.value,
                        'error': f"JSON decode error: {e}",
                        'validation': {'is_valid': False, 'issues': ['JSON malformato']}
                    })
            
            consumer.close()
            
            # Statistiche generali
            total_messages = len(messages)
            valid_messages = sum(1 for m in messages if m.get('validation', {}).get('is_valid', False))
            avg_size = sum(m.get('size_bytes', 0) for m in messages) / total_messages if total_messages > 0 else 0
            
            return {
                'total_messages': total_messages,
                'valid_messages': valid_messages,
                'invalid_messages': total_messages - valid_messages,
                'validity_rate': (valid_messages / total_messages * 100) if total_messages > 0 else 0,
                'average_size_bytes': avg_size,
                'messages': messages
            }
            
        except Exception as e:
            return {'error': str(e), 'total_messages': 0}
    
    def generate_spark_compatibility_report(self, kafka_analysis: Dict) -> Dict:
        """Genera report compatibilità Spark"""
        print("⚡ Analisi compatibilità Spark...")
        
        if kafka_analysis.get('error') or kafka_analysis.get('total_messages', 0) == 0:
            return {'compatible': False, 'reason': 'Nessun messaggio da analizzare'}
        
        messages = kafka_analysis.get('messages', [])
        compatibility_issues = []
        
        # Verifica schema consistency
        field_sets = []
        for msg in messages:
            if 'parsed_data' in msg:
                field_sets.append(set(msg['parsed_data'].keys()))
        
        if field_sets:
            common_fields = set.intersection(*field_sets) if len(field_sets) > 1 else field_sets[0]
            all_fields = set.union(*field_sets)
            
            if len(common_fields) < 3:
                compatibility_issues.append("Campi comuni insufficienti tra messaggi")
            
            if len(all_fields) - len(common_fields) > 2:
                compatibility_issues.append("Schema inconsistente tra messaggi")
        
        # Verifica tipi di dati
        numeric_fields = ['current_price', 'change', 'percent_change', 'high', 'low', 'open']
        for msg in messages[:5]:  # Controlla primi 5
            if 'parsed_data' in msg:
                data = msg['parsed_data']
                for field in numeric_fields:
                    if field in data and not isinstance(data[field], (int, float)):
                        compatibility_issues.append(f"Campo {field} non numerico: {type(data[field])}")
        
        return {
            'compatible': len(compatibility_issues) == 0,
            'issues': compatibility_issues,
            'common_fields': list(common_fields) if field_sets else [],
            'recommendation': 'OK per Spark' if len(compatibility_issues) == 0 else 'Richiede correzioni'
        }
    
    def print_detailed_analysis(self, kafka_analysis: Dict, spark_report: Dict):
        """Stampa analisi dettagliata"""
        print("\n" + "="*60)
        print("📊 ANALISI DETTAGLIATA FORMATO MESSAGGI")
        print("="*60)
        
        # Statistiche generali
        total = kafka_analysis.get('total_messages', 0)
        valid = kafka_analysis.get('valid_messages', 0)
        validity_rate = kafka_analysis.get('validity_rate', 0)
        
        print(f"\n📈 STATISTICHE GENERALI:")
        print(f"   Messaggi totali: {total}")
        print(f"   Messaggi validi: {valid}")
        print(f"   Tasso validità: {validity_rate:.1f}%")
        print(f"   Dimensione media: {kafka_analysis.get('average_size_bytes', 0):.0f} bytes")
        
        # Analisi per messaggio
        messages = kafka_analysis.get('messages', [])
        if messages:
            print(f"\n🔍 ANALISI PRIMI {min(3, len(messages))} MESSAGGI:")
            
            for i, msg in enumerate(messages[:3], 1):
                print(f"\n   📨 Messaggio #{i}:")
                if 'parsed_data' in msg:
                    data = msg['parsed_data']
                    print(f"      Symbol: {data.get('symbol', 'N/A')}")
                    print(f"      Price: ${data.get('current_price', 'N/A')}")
                    print(f"      Timestamp: {data.get('timestamp', 'N/A')}")
                    print(f"      Campi totali: {len(data)}")
                    
                    validation = msg.get('validation', {})
                    if validation.get('is_valid'):
                        print(f"      Status: ✅ VALIDO")
                    else:
                        print(f"      Status: ❌ PROBLEMI")
                        for issue in validation.get('issues', [])[:2]:
                            print(f"         - {issue}")
                else:
                    print(f"      ❌ Errore parsing: {msg.get('error', 'Sconosciuto')}")
        
        # Compatibilità Spark
        print(f"\n⚡ COMPATIBILITÀ SPARK:")
        if spark_report.get('compatible'):
            print(f"   ✅ COMPATIBILE - {spark_report.get('recommendation')}")
        else:
            print(f"   ❌ NON COMPATIBILE")
            for issue in spark_report.get('issues', []):
                print(f"      - {issue}")
        
        # Campi comuni
        common_fields = spark_report.get('common_fields', [])
        if common_fields:
            print(f"   📋 Campi comuni: {', '.join(common_fields)}")
    
    def run_complete_analysis(self):
        """Esegue analisi completa"""
        print("🚀 Portfolio Risk Analyzer - Analisi Formato Messaggi Completa")
        print("="*70)
        
        # Test API (se disponibile)
        try:
            with open('.env', 'r') as f:
                env_content = f.read()
                api_key_match = re.search(r'FINNHUB_API_KEY=(.+)', env_content)
                if api_key_match:
                    api_key = api_key_match.group(1).strip()
                    api_analysis = self.analyze_api_response(api_key)
                    
                    if api_analysis.get('status') == 'OK':
                        print("✅ API Finnhub risponde correttamente")
                        print(f"   Campi API: {', '.join(api_analysis['fields'])}")
                    else:
                        print(f"⚠️  Problema API: {api_analysis.get('message', 'Errore sconosciuto')}")
        except Exception as e:
            print(f"ℹ️  API test saltato: {e}")
        
        # Analisi Kafka
        kafka_analysis = self.analyze_kafka_messages(max_messages=10)
        
        if kafka_analysis.get('error'):
            print(f"❌ Errore analisi Kafka: {kafka_analysis['error']}")
            return
        
        if kafka_analysis.get('total_messages', 0) == 0:
            print("⚠️  Nessun messaggio trovato in Kafka")
            print("\n💡 Possibili cause:")
            print("   - Logstash non sta inviando dati")
            print("   - Topic vuoto") 
            print("   - Problemi di connettività")
            return
        
        # Report compatibilità Spark
        spark_report = self.generate_spark_compatibility_report(kafka_analysis)
        
        # Output dettagliato
        self.print_detailed_analysis(kafka_analysis, spark_report)
        
        # Raccomandazioni finali
        print(f"\n🎯 RACCOMANDAZIONI:")
        validity_rate = kafka_analysis.get('validity_rate', 0)
        
        if validity_rate >= 90 and spark_report.get('compatible'):
            print("   🎉 FORMATO MESSAGGI ECCELLENTE!")
            print("   ✅ Spark può processare i dati senza problemi")
            print("   ✅ Procedi con la pipeline completa")
        elif validity_rate >= 70:
            print("   🔧 FORMATO MESSAGGI BUONO MA MIGLIORABILE")
            print("   ⚠️  Alcuni messaggi hanno problemi minori")
            print("   💡 Spark dovrebbe funzionare ma monitora gli errori")
        else:
            print("   🚨 FORMATO MESSAGGI PROBLEMATICO")
            print("   ❌ Molti messaggi non validi")
            print("   🔧 Correggi Logstash prima di procedere")
            
            # Suggerimenti specifici
            most_common_issues = {}
            for msg in kafka_analysis.get('messages', []):
                for issue in msg.get('validation', {}).get('issues', []):
                    most_common_issues[issue] = most_common_issues.get(issue, 0) + 1
            
            if most_common_issues:
                print("\n   🔍 Problemi più comuni:")
                for issue, count in sorted(most_common_issues.items(), key=lambda x: x[1], reverse=True)[:3]:
                    print(f"      - {issue} ({count} volte)")

def main():
    analyzer = MessageFormatAnalyzer()
    analyzer.run_complete_analysis()

if __name__ == "__main__":
    main()