#!/usr/bin/env python3
"""
Test script per verificare la connessione all'API Finnhub
"""
import os
import requests
import json
from dotenv import load_dotenv

# Carica le variabili dal file .env
load_dotenv()

def test_finnhub_api():
    """Testa la connessione all'API Finnhub"""
    
    api_key = os.getenv('FINNHUB_API_KEY')
    if not api_key:
        print("❌ ERRORE: FINNHUB_API_KEY non trovata nel file .env")
        return False
    
    # ETF da testare
    test_symbol = "SPY"
    
    # URL per ottenere il prezzo in tempo reale
    url = f"https://finnhub.io/api/v1/quote"
    params = {
        'symbol': test_symbol,
        'token': api_key
    }
    
    try:
        print(f"🔍 Testando API Finnhub con simbolo {test_symbol}...")
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            # Verifica che abbiamo ricevuto dati validi
            if 'c' in data and data['c'] != 0:
                print("✅ API Finnhub funziona correttamente!")
                print(f"📊 Dati ricevuti per {test_symbol}:")
                print(f"   - Prezzo corrente: ${data['c']:.2f}")
                print(f"   - Variazione: ${data['d']:.2f} ({data['dp']:.2f}%)")
                print(f"   - Prezzo massimo: ${data['h']:.2f}")
                print(f"   - Prezzo minimo: ${data['l']:.2f}")
                print(f"   - Prezzo apertura: ${data['o']:.2f}")
                return True
            else:
                print("⚠️  API risponde ma i dati sembrano vuoti")
                print(f"Risposta: {json.dumps(data, indent=2)}")
                return False
                
        elif response.status_code == 401:
            print("❌ ERRORE: API Key non valida (401 Unauthorized)")
            return False
        elif response.status_code == 429:
            print("❌ ERRORE: Troppi richieste (429 Too Many Requests)")
            print("Prova ad aspettare un minuto e riprova")
            return False
        else:
            print(f"❌ ERRORE: Status code {response.status_code}")
            print(f"Risposta: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ ERRORE: Timeout nella richiesta")
        return False
    except requests.exceptions.RequestException as e:
        print(f"❌ ERRORE di connessione: {e}")
        return False
    except json.JSONDecodeError:
        print("❌ ERRORE: Risposta non è JSON valido")
        return False

def test_multiple_etfs():
    """Testa tutti gli ETF configurati"""
    
    etf_symbols = os.getenv('ETF_SYMBOLS', 'SPY,QQQ,IWM').split(',')
    api_key = os.getenv('FINNHUB_API_KEY')
    
    print(f"\n🔍 Testando tutti gli ETF: {', '.join(etf_symbols)}")
    
    for symbol in etf_symbols:
        symbol = symbol.strip()
        url = f"https://finnhub.io/api/v1/quote"
        params = {'symbol': symbol, 'token': api_key}
        
        try:
            response = requests.get(url, params=params, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if 'c' in data and data['c'] != 0:
                    print(f"   ✅ {symbol}: ${data['c']:.2f}")
                else:
                    print(f"   ⚠️  {symbol}: Dati non disponibili")
            else:
                print(f"   ❌ {symbol}: Errore {response.status_code}")
        except Exception as e:
            print(f"   ❌ {symbol}: Errore di connessione")

if __name__ == "__main__":
    print("🚀 Portfolio Risk Analyzer - Test API Finnhub\n")
    
    # Test base
    if test_finnhub_api():
        print("\n" + "="*50)
        # Test multipli ETF
        test_multiple_etfs()
        print("\n✅ Setup API completato con successo!")
    else:
        print("\n❌ Risolvi i problemi API prima di continuare.")
        print("\n💡 Suggerimenti:")
        print("   - Verifica che la tua API key sia corretta nel file .env")
        print("   - Controlla di avere ancora chiamate API disponibili")
        print("   - Visita https://finnhub.io/dashboard per verificare il tuo account")