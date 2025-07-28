#!/usr/bin/env python3
"""
Portfolio ETF Machine Learning Processor
Implementa ML in tempo reale per trading signals e anomaly detection
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.ml.clustering import KMeans
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator

class ETFMLProcessor:
    def __init__(self):
        self.app_name = "ETF Machine Learning Processor"
        self.spark = None
        self.kafka_servers = "kafka:29092"
        self.kafka_topic = "financial_prices"
        self.models = {}
        self.feature_history = []
        
    def create_spark_session(self):
        """Crea sessione Spark con MLlib"""
        try:
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-ml-checkpoints") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.adaptive.enabled", "true") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            print(f"✅ Spark ML Session creata: {self.app_name}")
            return True
            
        except Exception as e:
            print(f"❌ Errore Spark Session: {e}")
            return False
    
    def define_schema(self):
        """Schema per dati ETF"""
        return StructType([
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
    
    def create_ml_features(self, df):
        """Crea features per machine learning"""
        
        # Time-based aggregations (compatibili con Structured Streaming)
        time_window = "5 minutes"
        
        # Aggregazioni per finestra temporale
        windowed_df = df \
            .withWatermark("processing_time", "10 minutes") \
            .groupBy(
                window(col("processing_time"), time_window),
                col("symbol")
            ) \
            .agg(
                avg("current_price").alias("avg_price"),
                max("current_price").alias("max_price"),
                min("current_price").alias("min_price"),
                stddev("current_price").alias("price_volatility"),
                avg("percent_change").alias("avg_return"),
                count("*").alias("data_points"),
                max("high").alias("period_high"),
                min("low").alias("period_low"),
                first("current_price").alias("period_open"),
                last("current_price").alias("period_close")
            ) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .drop("window")
        
        # Feature engineering
        ml_features_df = windowed_df \
            .withColumn(
                # Price momentum
                "price_momentum",
                (col("period_close") - col("period_open")) / col("period_open") * 100
            ) \
            .withColumn(
                # Intraday volatility
                "intraday_volatility", 
                (col("period_high") - col("period_low")) / col("avg_price") * 100
            ) \
            .withColumn(
                # Price range position (dove siamo nel range high-low)
                "price_position",
                (col("period_close") - col("period_low")) / 
                (col("period_high") - col("period_low"))
            ) \
            .withColumn(
                # Volume proxy (data points come proxy del volume)
                "volume_proxy", col("data_points")
            ) \
            .withColumn(
                # Bollinger position (simplified)
                "bollinger_position",
                (col("period_close") - col("avg_price")) / col("price_volatility")
            ) \
            .withColumn(
                # Trend strength
                "trend_strength",
                abs(col("price_momentum")) * col("volume_proxy")
            ) \
            .withColumn(
                # Volatility regime (high/low)
                "volatility_regime",
                when(col("price_volatility") > 2.0, "high")
                .when(col("price_volatility") < 0.5, "low")
                .otherwise("normal")
            ) \
            .withColumn(
                # Target variable: next period direction
                "target_direction",
                when(col("price_momentum") > 0.5, 1.0)  # UP
                .when(col("price_momentum") < -0.5, 0.0)  # DOWN
                .otherwise(0.5)  # NEUTRAL
            )
        
        return ml_features_df
    
    def prepare_ml_dataset(self, df, epoch_id):
        """Prepara dataset per ML e addestra modelli"""
        
        if df.count() == 0:
            return
            
        print(f"\n🤖 ML Processing - Batch {epoch_id}")
        print("=" * 50)
        
        # Converti in Pandas per elaborazione più semplice
        pandas_df = df.toPandas()
        
        if len(pandas_df) < 10:
            print("⚠️  Dati insufficienti per ML (< 10 samples)")
            return
        
        # Feature selection per ML
        feature_columns = [
            'price_momentum', 'intraday_volatility', 'price_position', 
            'volume_proxy', 'trend_strength'
        ]
        
        # Verifica che tutte le feature siano presenti
        available_features = [col for col in feature_columns if col in pandas_df.columns]
        
        if len(available_features) < 3:
            print("⚠️  Feature insufficienti per ML")
            return
        
        # Addestra modelli per ogni simbolo
        symbols = pandas_df['symbol'].unique()
        
        for symbol in symbols:
            symbol_data = pandas_df[pandas_df['symbol'] == symbol]
            
            if len(symbol_data) >= 5:
                self.train_symbol_models(symbol, symbol_data, available_features, epoch_id)
        
        # Cross-ETF analysis
        if len(symbols) >= 2:
            self.cross_etf_analysis(pandas_df, available_features, epoch_id)
    
    def train_symbol_models(self, symbol, data, features, epoch_id):
        """Addestra modelli ML per un singolo simbolo"""
        import pandas as pd
        import numpy as np
        from sklearn.ensemble import IsolationForest, RandomForestClassifier
        from sklearn.preprocessing import StandardScaler
        from sklearn.model_selection import train_test_split
        
        print(f"\n🎯 Modelli ML per {symbol}")
        print("-" * 30)
        
        # Prepara feature matrix
        X = data[features].fillna(0).values
        
        if len(X) < 5:
            print(f"   ⚠️  Dati insufficienti per {symbol}")
            return
        
        # 1. ANOMALY DETECTION
        try:
            isolation_forest = IsolationForest(
                contamination=0.1, 
                random_state=42,
                n_estimators=50
            )
            anomaly_scores = isolation_forest.fit_predict(X)
            anomaly_count = np.sum(anomaly_scores == -1)
            
            print(f"   🚨 Anomalie rilevate: {anomaly_count}/{len(X)}")
            
            # Salva modello anomaly detection
            self.models[f"{symbol}_anomaly"] = isolation_forest
            
        except Exception as e:
            print(f"   ❌ Errore anomaly detection: {e}")
        
        # 2. PRICE DIRECTION PREDICTION
        try:
            # Target: direzione del momentum
            y = (data['price_momentum'] > 0).astype(int).values
            
            if len(np.unique(y)) > 1 and len(X) >= 8:  # Almeno 2 classi
                X_train, X_test, y_train, y_test = train_test_split(
                    X, y, test_size=0.3, random_state=42
                )
                
                # Random Forest Classifier
                rf_classifier = RandomForestClassifier(
                    n_estimators=20, 
                    max_depth=3, 
                    random_state=42
                )
                rf_classifier.fit(X_train, y_train)
                
                # Accuracy sul test set
                if len(X_test) > 0:
                    accuracy = rf_classifier.score(X_test, y_test)
                    print(f"   📈 Accuracy predizione: {accuracy:.3f}")
                
                # Feature importance
                importances = rf_classifier.feature_importances_
                for i, feature in enumerate(features):
                    if i < len(importances):
                        print(f"      {feature}: {importances[i]:.3f}")
                
                # Salva modello
                self.models[f"{symbol}_direction"] = rf_classifier
                
        except Exception as e:
            print(f"   ❌ Errore direction prediction: {e}")
        
        # 3. VOLATILITY PREDICTION
        try:
            # Target: volatilità futura
            y_vol = data['intraday_volatility'].values
            
            if len(X) >= 6:
                from sklearn.ensemble import RandomForestRegressor
                
                rf_regressor = RandomForestRegressor(
                    n_estimators=20,
                    max_depth=3,
                    random_state=42
                )
                rf_regressor.fit(X, y_vol)
                
                # R² score
                r2_score = rf_regressor.score(X, y_vol)
                print(f"   📊 R² volatilità: {r2_score:.3f}")
                
                # Salva modello
                self.models[f"{symbol}_volatility"] = rf_regressor
                
        except Exception as e:
            print(f"   ❌ Errore volatility prediction: {e}")
    
    def cross_etf_analysis(self, data, features, epoch_id):
        """Analisi cross-ETF e regime detection"""
        import pandas as pd
        import numpy as np
        from sklearn.cluster import KMeans
        
        print(f"\n🔄 Cross-ETF Analysis")
        print("-" * 25)
        
        try:
            # Pivot data per symbol
            pivot_data = data.pivot_table(
                index='window_start',
                columns='symbol', 
                values='price_momentum',
                aggfunc='mean'
            ).fillna(0)
            
            if len(pivot_data) >= 3 and len(pivot_data.columns) >= 2:
                
                # Correlation analysis
                corr_matrix = pivot_data.corr()
                print("   🔗 Correlazioni attuali:")
                for i, sym1 in enumerate(corr_matrix.columns):
                    for j, sym2 in enumerate(corr_matrix.columns):
                        if i < j:
                            corr = corr_matrix.loc[sym1, sym2]
                            print(f"      {sym1}-{sym2}: {corr:.3f}")
                
                # Market regime clustering
                X_regime = pivot_data.values
                
                if len(X_regime) >= 3:
                    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
                    regime_labels = kmeans.fit_predict(X_regime)
                    
                    current_regime = regime_labels[-1]
                    regime_names = ["Bear", "Neutral", "Bull"]
                    
                    print(f"   📊 Regime attuale: {regime_names[current_regime]}")
                    print(f"   📈 Regime distribution: {np.bincount(regime_labels)}")
                    
                    # Salva modello regime
                    self.models["market_regime"] = kmeans
                
        except Exception as e:
            print(f"   ❌ Errore cross-ETF analysis: {e}")
    
    def make_predictions(self, df, epoch_id):
        """Fa predizioni usando i modelli addestrati"""
        
        if not self.models:
            return
        
        print(f"\n🔮 PREDIZIONI ML - Batch {epoch_id}")
        print("=" * 40)
        
        pandas_df = df.toPandas()
        
        feature_columns = [
            'price_momentum', 'intraday_volatility', 'price_position', 
            'volume_proxy', 'trend_strength'
        ]
        
        for symbol in pandas_df['symbol'].unique():
            symbol_data = pandas_df[pandas_df['symbol'] == symbol]
            
            if len(symbol_data) == 0:
                continue
                
            print(f"\n🏷️  {symbol} - Predizioni:")
            
            # Prepara features
            try:
                X = symbol_data[feature_columns].fillna(0).values
                
                if len(X) == 0:
                    continue
                
                latest_features = X[-1:] if len(X) > 0 else None
                
                # Anomaly detection
                if f"{symbol}_anomaly" in self.models and latest_features is not None:
                    anomaly_pred = self.models[f"{symbol}_anomaly"].predict(latest_features)[0]
                    anomaly_status = "🚨 ANOMALIA" if anomaly_pred == -1 else "✅ Normale"
                    print(f"   Anomaly: {anomaly_status}")
                
                # Direction prediction
                if f"{symbol}_direction" in self.models and latest_features is not None:
                    direction_prob = self.models[f"{symbol}_direction"].predict_proba(latest_features)[0]
                    direction = "📈 UP" if direction_prob[1] > 0.6 else "📉 DOWN" if direction_prob[0] > 0.6 else "➡️ NEUTRALE"
                    confidence = max(direction_prob)
                    print(f"   Direzione: {direction} (conf: {confidence:.2f})")
                
                # Volatility prediction
                if f"{symbol}_volatility" in self.models and latest_features is not None:
                    vol_pred = self.models[f"{symbol}_volatility"].predict(latest_features)[0]
                    print(f"   Vol. prevista: {vol_pred:.2f}%")
                
                # Current price per context
                current_price = symbol_data['period_close'].iloc[-1]
                momentum = symbol_data['price_momentum'].iloc[-1]
                print(f"   💰 Prezzo: ${current_price:.2f} ({momentum:+.2f}%)")
                
            except Exception as e:
                print(f"   ❌ Errore predizioni {symbol}: {e}")
        
        # Market regime
        if "market_regime" in self.models:
            try:
                pivot_data = pandas_df.pivot_table(
                    index='window_start',
                    columns='symbol', 
                    values='price_momentum',
                    aggfunc='mean'
                ).fillna(0)
                
                if len(pivot_data) > 0:
                    latest_regime_data = pivot_data.values[-1:] 
                    regime_pred = self.models["market_regime"].predict(latest_regime_data)[0]
                    regime_names = ["🐻 Bear Market", "😐 Neutral", "🐂 Bull Market"]
                    print(f"\n🌍 Market Regime: {regime_names[regime_pred]}")
                    
            except Exception as e:
                print(f"❌ Errore regime prediction: {e}")
    
    def save_ml_results(self, results, epoch_id):
        """Salva risultati ML"""
        import json
        import pickle
        
        try:
            timestamp = datetime.now().isoformat()
            
            # Salva risultati
            results_data = {
                'timestamp': timestamp,
                'batch_id': epoch_id,
                'ml_results': results,
                'models_count': len(self.models)
            }
            
            os.makedirs('/opt/spark/data/ml_results', exist_ok=True)
            
            # JSON results
            json_file = f"/opt/spark/data/ml_results/ml_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(json_file, 'w') as f:
                json.dump(results_data, f, indent=2, default=str)
            
            # Pickle models
            if self.models:
                model_file = f"/opt/spark/data/ml_results/models_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"
                with open(model_file, 'wb') as f:
                    pickle.dump(self.models, f)
            
            print(f"💾 ML Results salvati: {json_file}")
            
        except Exception as e:
            print(f"⚠️  Errore salvataggio ML: {e}")
    
    def start_ml_processing(self):
        """Avvia ML processing"""
        if not self.create_spark_session():
            return
        
        print(f"🚀 Avvio ETF Machine Learning Processor...")
        print(f"🤖 Modelli: Anomaly Detection, Direction Prediction, Volatility Forecast")
        print(f"📊 Cross-ETF: Correlation Analysis, Market Regime Detection")
        
        # Leggi da Kafka
        etf_schema = self.define_schema()
        
        kafka_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse e filtra
        parsed_stream = kafka_stream.select(
            col("key").cast("string").alias("kafka_key"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), etf_schema).alias("data")
        ).select("kafka_key", "kafka_timestamp", "data.*")
        
        valid_stream = parsed_stream.filter(
            (col("current_price") > 0) &
            (col("symbol").isNotNull()) &
            (col("symbol").isin(["SPY", "QQQ", "IWM"]))
        ).withColumn("processing_time", current_timestamp())
        
        # Crea ML features
        ml_features = self.create_ml_features(valid_stream)
        
        # Query 1: Train models
        train_query = ml_features.writeStream \
            .outputMode("append") \
            .foreachBatch(self.prepare_ml_dataset) \
            .option("checkpointLocation", "/tmp/spark-ml-train-checkpoint") \
            .trigger(processingTime="120 seconds") \
            .start()
        
        # Query 2: Make predictions  
        predict_query = ml_features.writeStream \
            .outputMode("append") \
            .foreachBatch(self.make_predictions) \
            .option("checkpointLocation", "/tmp/spark-ml-predict-checkpoint") \
            .trigger(processingTime="90 seconds") \
            .start()
        
        # Query 3: Console monitoring
        console_query = ml_features.select(
            "symbol", "price_momentum", "intraday_volatility", 
            "trend_strength", "volatility_regime", "window_start"
        ).writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 5) \
            .trigger(processingTime="60 seconds") \
            .start()
        
        print("✅ ETF ML Processing avviato!")
        print("🤖 Tre query ML attive:")
        print("   1. 🎓 Training modelli → ogni 120 secondi")
        print("   2. 🔮 Predizioni → ogni 90 secondi")
        print("   3. 🖥️  Monitor → ogni 60 secondi")
        print("🛑 Premi Ctrl+C per fermare")
        
        try:
            train_query.awaitTermination()
        except KeyboardInterrupt:
            print("\n🛑 Interruzione ricevuta...")
            train_query.stop()
            predict_query.stop()
            console_query.stop()
        except Exception as e:
            print(f"❌ Errore ML: {e}")
        finally:
            if self.spark:
                self.spark.stop()
            print("✅ ML Processor terminato")

def main():
    processor = ETFMLProcessor()
    processor.start_ml_processing()

if __name__ == "__main__":
    main()