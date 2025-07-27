# Parte 0

## Environment Setup
### API

Innanzitutto, è necessario scegliere l’API su cui basare il progetto. **Alpha Vintage** fornisce un limite stringente di cinque chiamate al minuto; essendo comunque il più conveniente, scegliamo questa come API.

### Portfolio per Testing

Successivamente, sarà necessario scegliere un piccolo portafoglio realistico. Scegliamo i seguenti **cinque ETF**:

- SPY (S&P 500)
- QQQ (NASDAQ)
- IWM (Russell 2000)
- EEM (Emerging Markets)
- TLT (Treasury Bonds)

> *Possiamo pensare di incrementare a dieci ETF, includendo anche settoriali (XLF, XLK, XLE...).*
> 


### Ambiente
Per il setup dell’ambiente, utilizzeremo ***Docker Desktop.*** 

Questa sarà l’organizzazione per la nostra repository:

```
portfolio-risk-analyzer/
├── docker-compose.yml
├── .env               # credenziali sicure
├── logstash/
│   ├── pipeline/
│   └── config/
├── spark/
│   ├── jobs/
│   └── data/
├── grafana/
│   └── dashboards/
└── scripts/
    └── utils/

```

Logstash è un pipeline di elaborazione dati open-source lato server che ti permette di ingerire dati da una moltitudine di fonti, trasformarli dinamicamente e inviarli a una varietà di destinazioni. Fa parte della Elastic Stack (ELK: Elasticsearch, Logstash, Kibana, a cui aggiungiamo Kafka e Spark).

# Parte 1
