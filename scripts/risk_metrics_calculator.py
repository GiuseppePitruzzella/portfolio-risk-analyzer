#!/usr/bin/env python3
"""
Risk Metrics Calculator per Portfolio Risk Analyzer
Questo script calcola VaR, CVaR, Tracking Error e Correlazione
utilizzando i dati storici salvati in Elasticsearch.
"""

import os
import sys
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

import requests
from elasticsearch import Elasticsearch

class RiskMetricsCalculator:
    def __init__(self):
        """Inizializza il calcolatore di metriche di rischio"""
        self.app_name = "Portfolio Risk Analyzer - Risk Metrics"
        self.spark = None
        self.es_client = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        self.benchmark_symbol = "SPY"  # SPY come benchmark per tracking error
        
    def create_spark_session(self):
        """Crea sessione Spark per elaborazioni batch"""
        try:
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            print(f"✅ Spark Session creata per calcoli di rischio")
            return True
            
        except Exception as e:
            print(f"❌ Errore Spark Session: {e}")
            return False
    
    def get_historical_data(self, days_back: int = 30) -> Optional[pd.DataFrame]:
        """Recupera dati storici da Elasticsearch"""
        try:
            # Calcola il range di date
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            # Query Elasticsearch per dati storici
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "processing_time": {
                                        "gte": start_date.isoformat(),
                                        "lte": end_date.isoformat()
                                    }
                                }
                            },
                            {
                                "terms": {
                                    "symbol": ["SPY", "QQQ", "IWM"]
                                }
                            }
                        ]
                    }
                },
                "sort": [
                    {"processing_time": {"order": "asc"}}
                ],
                "size": 10000  # Massimo record da recuperare
            }
            
            # Cerca in tutti gli indici ETF del periodo
            index_pattern = "etf-prices-*"
            
            print(f"🔍 Recupero dati storici da {start_date.date()} a {end_date.date()}")
            
            response = self.es_client.search(
                index=index_pattern,
                body=query
            )
            
            hits = response['hits']['hits']
            
            if not hits:
                print("⚠️  Nessun dato storico trovato in Elasticsearch")
                return None
            
            # Converti in DataFrame pandas
            data = []
            for hit in hits:
                source = hit['_source']
                data.append({
                    'symbol': source.get('symbol'),
                    'current_price': source.get('current_price'),
                    'percent_change': source.get('percent_change', 0),
                    'processing_time': source.get('processing_time'),
                    'volatility_score': source.get('volatility_score', 0),
                    'price_range_pct': source.get('price_range_pct', 0)
                })
            
            df = pd.DataFrame(data)
            df['processing_time'] = pd.to_datetime(df['processing_time'])
            df = df.sort_values(['symbol', 'processing_time']).reset_index(drop=True)
            
            print(f"✅ Recuperati {len(df)} record per {df['symbol'].nunique()} simboli")
            return df
            
        except Exception as e:
            print(f"❌ Errore nel recupero dati storici: {e}")
            return None
    
    def calculate_returns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcola i rendimenti percentuali per ogni simbolo"""
        try:
            results = []
            
            for symbol in df['symbol'].unique():
                symbol_data = df[df['symbol'] == symbol].copy()
                symbol_data = symbol_data.sort_values('processing_time')
                
                # Calcola rendimenti percentuali
                symbol_data['returns'] = symbol_data['current_price'].pct_change() * 100
                symbol_data = symbol_data.dropna()
                
                results.append(symbol_data)
            
            return pd.concat(results, ignore_index=True)
            
        except Exception as e:
            print(f"❌ Errore nel calcolo dei rendimenti: {e}")
            return df
    
    def calculate_var_cvar(self, returns: np.array, confidence_level: float = 0.95) -> Tuple[float, float]:
        """Calcola Value at Risk (VaR) e Conditional VaR (CVaR)"""
        try:
            if len(returns) < 10:
                return 0.0, 0.0
            
            # Ordina i rendimenti
            sorted_returns = np.sort(returns)
            
            # Calcola VaR (percentile)
            var_index = int((1 - confidence_level) * len(sorted_returns))
            var = sorted_returns[var_index] if var_index < len(sorted_returns) else sorted_returns[-1]
            
            # Calcola CVaR (media dei rendimenti peggiori del VaR)
            cvar_returns = sorted_returns[:var_index+1]
            cvar = np.mean(cvar_returns) if len(cvar_returns) > 0 else var
            
            return abs(var), abs(cvar)  # Ritorna valori positivi per convenzione
            
        except Exception as e:
            print(f"❌ Errore nel calcolo VaR/CVaR: {e}")
            return 0.0, 0.0
    
    def calculate_tracking_error(self, etf_returns: np.array, benchmark_returns: np.array) -> float:
        """Calcola il Tracking Error rispetto al benchmark"""
        try:
            if len(etf_returns) != len(benchmark_returns) or len(etf_returns) < 10:
                return 0.0
            
            # Calcola le differenze dei rendimenti
            tracking_diff = etf_returns - benchmark_returns
            
            # Tracking Error = deviazione standard delle differenze
            tracking_error = np.std(tracking_diff, ddof=1)
            
            return tracking_error
            
        except Exception as e:
            print(f"❌ Errore nel calcolo Tracking Error: {e}")
            return 0.0
    
    def calculate_correlation_matrix(self, df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
        """Calcola la matrice di correlazione tra gli ETF"""
        try:
            # Pivot per avere i simboli come colonne
            returns_pivot = df.pivot_table(
                index='processing_time',
                columns='symbol',
                values='returns',
                aggfunc='mean'
            ).fillna(0)
            
            # Calcola matrice di correlazione
            correlation_matrix = returns_pivot.corr()
            
            # Converti in dizionario
            corr_dict = {}
            for symbol1 in correlation_matrix.columns:
                corr_dict[symbol1] = {}
                for symbol2 in correlation_matrix.columns:
                    corr_dict[symbol1][symbol2] = float(correlation_matrix.loc[symbol1, symbol2])
            
            return corr_dict
            
        except Exception as e:
            print(f"❌ Errore nel calcolo correlazioni: {e}")
            return {}
    
    def calculate_all_metrics(self) -> Dict:
        """Calcola tutte le metriche di rischio"""
        print("🧮 Avvio calcolo metriche di rischio")
        print("=" * 50)
        
        # Recupera dati storici
        historical_data = self.get_historical_data(days_back=30)
        if historical_data is None:
            return {}
        
        # Calcola rendimenti
        print("📊 Calcolo rendimenti...")
        returns_data = self.calculate_returns(historical_data)
        
        # Inizializza risultati
        results = {
            'calculation_time': datetime.now().isoformat(),
            'data_period_days': 30,
            'metrics': {}
        }
        
        # Calcola metriche per ogni simbolo
        symbols = returns_data['symbol'].unique()
        benchmark_returns = None
        
        # Prima ottieni i rendimenti del benchmark
        if self.benchmark_symbol in symbols:
            benchmark_data = returns_data[returns_data['symbol'] == self.benchmark_symbol]
            benchmark_returns = benchmark_data['returns'].values
        
        for symbol in symbols:
            print(f"🔍 Analisi {symbol}...")
            
            symbol_data = returns_data[returns_data['symbol'] == symbol]
            symbol_returns = symbol_data['returns'].values
            
            if len(symbol_returns) < 10:
                print(f"   ⚠️  Dati insufficienti per {symbol}")
                continue
            
            metrics = {}
            
            # VaR e CVaR
            var_95, cvar_95 = self.calculate_var_cvar(symbol_returns, 0.95)
            var_99, cvar_99 = self.calculate_var_cvar(symbol_returns, 0.99)
            
            metrics['var'] = {
                'var_95': round(var_95, 4),
                'var_99': round(var_99, 4),
                'cvar_95': round(cvar_95, 4),
                'cvar_99': round(cvar_99, 4)
            }
            
            # Tracking Error (solo se non è il benchmark stesso)
            if symbol != self.benchmark_symbol and benchmark_returns is not None:
                # Allinea le lunghezze degli array
                min_length = min(len(symbol_returns), len(benchmark_returns))
                te = self.calculate_tracking_error(
                    symbol_returns[-min_length:],
                    benchmark_returns[-min_length:]
                )
                metrics['tracking_error'] = round(te, 4)
            
            # Statistiche aggiuntive
            metrics['statistics'] = {
                'mean_return': round(np.mean(symbol_returns), 4),
                'volatility': round(np.std(symbol_returns, ddof=1), 4),
                'min_return': round(np.min(symbol_returns), 4),
                'max_return': round(np.max(symbol_returns), 4),
                'data_points': len(symbol_returns)
            }
            
            results['metrics'][symbol] = metrics
            
            print(f"   ✅ VaR 95%: {var_95:.4f}%, CVaR 95%: {cvar_95:.4f}%")
            if 'tracking_error' in metrics:
                print(f"   ✅ Tracking Error: {metrics['tracking_error']:.4f}%")
        
        # Calcola matrice di correlazione
        print("🔗 Calcolo correlazioni...")
        correlation_matrix = self.calculate_correlation_matrix(returns_data)
        results['correlation_matrix'] = correlation_matrix
        
        return results
    
    def save_metrics_to_elasticsearch(self, metrics: Dict):
        """Salva le metriche calcolate in Elasticsearch"""
        try:
            # Indice per le metriche di rischio
            index_name = f"risk-metrics-{datetime.now().strftime('%Y-%m')}"
            
            # Documento con timestamp
            doc = {
                **metrics,
                'document_id': f"risk_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            }
            
            # Salva in Elasticsearch
            response = self.es_client.index(
                index=index_name,
                body=doc
            )
            
            print(f"✅ Metriche salvate in Elasticsearch: {index_name}")
            return True
            
        except Exception as e:
            print(f"❌ Errore nel salvataggio in Elasticsearch: {e}")
            return False
    
    def run_risk_analysis(self):
        """Esegue l'analisi completa del rischio"""
        print("🚀 Portfolio Risk Analyzer - Calcolo Metriche di Rischio")
        print("=" * 60)
        
        if not self.create_spark_session():
            return
        
        try:
            # Calcola metriche
            metrics = self.calculate_all_metrics()
            
            if not metrics:
                print("❌ Nessuna metrica calcolata")
                return
            
            # Stampa risultati
            self.print_risk_summary(metrics)
            
            # Salva in Elasticsearch
            self.save_metrics_to_elasticsearch(metrics)
            
            # Salva in file JSON per backup
            with open(f"data/risk_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 'w') as f:
                json.dump(metrics, f, indent=2)
            
            print("✅ Analisi del rischio completata!")
            
        except Exception as e:
            print(f"❌ Errore nell'analisi del rischio: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if self.spark:
                self.spark.stop()
    
    def print_risk_summary(self, metrics: Dict):
        """Stampa un riassunto delle metriche di rischio"""
        print("\n📊 RIASSUNTO METRICHE DI RISCHIO")
        print("=" * 60)
        
        for symbol, data in metrics.get('metrics', {}).items():
            print(f"\n🏷️  {symbol}")
            print("-" * 30)
            
            # VaR e CVaR
            var_data = data.get('var', {})
            print(f"💰 VaR (95%): {var_data.get('var_95', 0):.4f}%")
            print(f"💰 VaR (99%): {var_data.get('var_99', 0):.4f}%")
            print(f"🔥 CVaR (95%): {var_data.get('cvar_95', 0):.4f}%")
            print(f"🔥 CVaR (99%): {var_data.get('cvar_99', 0):.4f}%")
            
            # Tracking Error
            if 'tracking_error' in data:
                print(f"📊 Tracking Error: {data['tracking_error']:.4f}%")
            
            # Statistiche
            stats = data.get('statistics', {})
            print(f"📈 Volatilità: {stats.get('volatility', 0):.4f}%")
            print(f"📋 Punti dati: {stats.get('data_points', 0)}")
        
        # Correlazioni
        print(f"\n🔗 MATRICE DI CORRELAZIONE")
        print("-" * 30)
        corr_matrix = metrics.get('correlation_matrix', {})
        if corr_matrix:
            symbols = list(corr_matrix.keys())
            print(f"{'':>6}", end="")
            for s in symbols:
                print(f"{s:>8}", end="")
            print()
            
            for s1 in symbols:
                print(f"{s1:>6}", end="")
                for s2 in symbols:
                    corr_val = corr_matrix.get(s1, {}).get(s2, 0)
                    print(f"{corr_val:>8.3f}", end="")
                print()

def main():
    calculator = RiskMetricsCalculator()
    calculator.run_risk_analysis()

if __name__ == "__main__":
    main()