<br />
<div align="center">
    <a href="https://github.com/GiuseppePitruzzella/portfolio-risk-analyzer">
        <img src="assets/images/logo.png" alt="Logo" width="300"> </a>

    <h3 align="center">Real-Time Portfolio Risk Sentinel</h3>

    <p align="center">
        A real-time analysis pipeline to monitor and manage ETF portfolio risk.
        <br />
        <a href="https://github.com/GiuseppePitruzzella/portfolio-risk-analyzer/issues">Report a Bug</a>
        ·
        <a href="https://github.com/GiuseppePitruzzella/portfolio-risk-analyzer/issues">Request a Feature</a>
    </p>
</div>

---

## 📘 Project Overview

This project aims to provide individual investors with an advanced tool for **real-time ETF portfolio risk management**. Leveraging a robust architecture based on **Logstash, Kafka, Spark, Elasticsearch, and Grafana**, the application continuously monitors portfolio positions, calculates risk metrics such as **Value at Risk (VaR)** and **drawdown**, and generates automatic alerts when user-defined risk limits are exceeded. The goal is to enable faster, more informed decisions to protect invested capital.

---

## 📈 Pipeline Architecture

The core of this "Risk Sentinel" is a scalable and resilient data pipeline designed for real-time processing:

* **Logstash:** Ingests market data (ETF prices) and portfolio position information from various sources.
* **Kafka:** Acts as a high-speed distributed message broker, ensuring resilience and data stream distribution.
* **Spark:** The distributed computation engine that processes streaming data to calculate VaR, drawdown, and perform stress tests on positions.
* **Elasticsearch:** Stores calculated risk data and alerts, enabling fast queries and historical archiving.
* **Grafana:** Provides interactive dashboards for real-time risk metric visualization and custom alert configuration.
* **REST/WebSocket API (future):** For integration with external applications (e.g., mobile, trading desk), allowing real-time data access.

---

## 🧪 Development Environment

The entire development environment and pipeline services are orchestrated via **Docker Compose**, ensuring reproducibility and easy setup.

### Prerequisites

Make sure you have installed:
* **Docker Desktop** (includes Docker Engine and Docker Compose)
* **Git**

### Local Setup

1.  **Clone the repository:**
        ```bash
        git clone https://github.com/GiuseppePitruzzella/portfolio-risk-analyzer.git
        cd portfolio-risk-analyzer
        ```
2.  **Configure Docker services:**
        The `docker-compose.yml` file defines all required services (Logstash, Kafka, Spark, Elasticsearch, Grafana). This file will be populated in upcoming project phases.
        ```bash
        # This command will be added once docker-compose.yml is defined
        # docker-compose up -d
        ```
3.  **Install Python dependencies:**
        Python dependencies for Spark applications and any API services will be listed in their respective `requirements.txt` files.
        ```bash
        # For main project dependencies (if present)
        pip install -r requirements.txt
        # For API-specific dependencies (if implemented)
        # pip install -r api/requirements.txt
        ```

---

## 📁 Project Structure

````

portfolio-risk-analyzer/
├── .gitignore               # File to ignore files and folders in Git
├── README.md                # Project description and instructions
├── requirements.txt         # General Python dependencies for the project
├── docker-compose.yml       # Docker service definitions (Kafka, Spark, ES, Grafana, Logstash)
├── docker/                  # Folder for custom Dockerfiles and configurations
│   ├── logstash/
│   │   └── Dockerfile
│   │   └── config/          # Logstash configuration (pipelines, inputs, filters, outputs)
│   ├── spark/
│   │   └── Dockerfile
│   │   └── scripts/         # Startup scripts for Spark workers (if needed)
├── spark_jobs/              # Source code for Spark applications
│   ├── real_time_risk_calc.py # Main Spark script for calculations
│   └── utils/
│       └── risk_calculations.py # Modular functions for VaR, Drawdown, Stress-Test
├── api/                     # Microservice for REST/WebSocket API (optional, e.g., Flask/FastAPI)
│   ├── app.py               # Main API logic
│   └── requirements.txt     # API-specific dependencies
│   └── Dockerfile           # Dockerfile for the API service
├── config/                  # Generic configuration files (e.g., default risk limits)
│   └── app_config.json
├── data/                    # Folder for sample data, non-volatile historical data
│   └── historical_etf_prices.csv # Historical data for training/backtesting
│   └── portfolio_positions.csv   # Example portfolio positions
├── docs/                    # Additional documentation (architecture, algorithms)
└── scripts/                 # Utility scripts (setup, test, deploy)
└── setup_kafka_topics.sh

```

---

## 🔒 `.gitignore` Strategy

To keep the repository clean and prevent accidental commits of unnecessary or sensitive files:

* Files generated during execution (logs, temporary outputs).
* Downloaded Python dependencies (`venv`, `__pycache__`).
* Sensitive or environment-specific configuration files (`.env`).
* Large data files not intended for version control (e.g., full historical datasets, if too large).
* OS-specific system files (e.g., `.DS_Store`).

---

## 📊 Historical Data for "Training"

To calculate risk metrics such as historical VaR and for future backtesting or stress tests, **historical ETF price data** is required.

You can obtain this data from several sources:

* **Free Sources:** Yahoo Finance (via CSV download or Python libraries like `yfinance`), Investing.com, or public data from some stock exchanges. These are ideal for getting started.
* **Paid (Professional) Sources:** For greater reliability, granularity (intraday/tick-by-tick data), and coverage, consider paid API services like Alpha Vantage, Twelve Data, IEX Cloud, or, for institutional use, Bloomberg/Refinitiv.

Historical data will be loaded and processed by Spark to derive the necessary risk parameters, which will then be used in real-time calculations.

---

## 📬 Contacts

[Giueppe Pitruzzella] – [@GiuseppePitruzzella](https://github.com/GiuseppePitruzzella)
Project Repository – [portfolio-risk-analyzer](https://github.com/GiuseppePitruzzella/portfolio-risk-analyzer)

