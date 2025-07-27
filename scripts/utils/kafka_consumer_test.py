#!/usr/bin/env python3
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'financial_prices',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening for messages...")
for message in consumer:
    print(f"[MSG] {message.value}")