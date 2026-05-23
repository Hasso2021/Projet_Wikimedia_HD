# Wikimedia Big Data — Plateforme Analytics

Projet **Master Big Data** : plateforme simplifiée d’analyse Wikimedia basée sur le flux public [recentchange](https://stream.wikimedia.org/v2/stream/recentchange).

**Pipeline** : ingestion → Kafka → Spark (Streaming + Batch) → HDFS → Airflow → Dashboard.

---

## Objectif du projet

1. Ingérer le flux Wikimedia en temps réel vers **4 topics Kafka**.
2. Transformer et stocker les événements dans un **Data Lake HDFS** (`hdfs://namenode:8020/data/wikimedia/`).
3. Produire des **agrégations analytiques** et des **rapports JSON**.
4. Détecter les **anomalies** (qualité + ops).
5. **Monitorer** les pipelines (Airflow, Kafka, Spark).
6. Visualiser via un **dashboard Streamlit** (historique, live, monitoring ops).

---

## Architecture

```text
Wikimedia EventStreams (SSE)
        ↓
Ingestion (Python + DAG Airflow wikimedia_ingestion)
        ↓
Kafka : wm.recentchange.raw | wm.bot.events | wm.page.edits | wm.errors
        ↓
Spark Structured Streaming  →  /data/wikimedia/processed/
Spark Batch (optionnel)     →  /data/wikimedia/batch/
        ↓
Airflow : agrégations | anomalies | reporting | monitoring
        ↓
HDFS : /data/wikimedia/reports/  +  /data/wikimedia/anomalies/
        ↓
Streamlit Dashboard (Historical | Live | Monitoring & Ops)
```

---

## Stack technique

| Composant | Technologie | Version |
|-----------|-------------|---------|
| Messaging | Apache Kafka | Confluent 7.5.3 |
| Stream / Batch | Apache Spark | 3.5.1 |
| Stockage | HDFS Hadoop 3.2 | bde2020 |
| Orchestration | Apache Airflow | 3.2.1 |
| Ingestion | Python | 3.11+ |
| Dashboard | Streamlit, Plotly | voir `dashboard/requirements.txt` |
| Conteneurisation | Docker Compose | v2 |

---

## Structure du projet

```text
.
├── ingestion/wikimedia_producer.py      # SSE → Kafka (4 topics)
├── spark/jobs/
│   ├── wikimedia_stream_consumer.py   # Streaming → HDFS
│   └── wikimedia_batch_processor.py   # Batch → HDFS
├── airflow/dags/
│   ├── wikimedia_ingestion_dag.py           # DAG 1 — ingestion
│   ├── wikimedia_global_activity_dag.py      # Agrégations activité
│   ├── wikimedia_top_pages_dag.py           # Top pages
│   ├── wikimedia_user_activity_dag.py       # Utilisateurs
│   ├── wikimedia_bot_activity_dag.py        # Bots
│   ├── wikimedia_anomaly_detection_dag.py   # DAG 4 — anomalies
│   ├── wikimedia_automated_reporting_dag.py # DAG 6 — reporting
│   ├── wikimedia_pipeline_monitoring_dag.py # DAG 7 — monitoring
│   └── wikimedia_hdfs_healthcheck_dag.py
├── dashboard/app.py                   # 3 modes UI
└── scripts/                           # Kafka, HDFS, DAGs, Spark batch
```

---

## Chemins HDFS

| Chemin | Contenu |
|--------|---------|
| `/data/wikimedia/processed/` | Événements transformés (Spark Streaming) |
| `/data/wikimedia/anomalies/` | Anomalies détectées (JSON) |
| `/data/wikimedia/reports/` | Rapports analytiques + `YYYY-MM-DD.json` |
| `/data/wikimedia/batch/` | Résumés Spark Batch |
| `/data/wikimedia/checkpoints/` | Checkpoints Spark |

Préparation : `.\scripts\prepare-hdfs-wikimedia.ps1`

---

## Installation et lancement Docker

```powershell
copy .env.example .env
docker compose pull
docker compose up -d
.\scripts\create-kafka-topics.ps1
.\scripts\prepare-hdfs-wikimedia.ps1
docker compose build dashboard
```

Interfaces : HDFS http://localhost:9870 | Airflow http://localhost:8088 | Dashboard http://localhost:8501

---

## 1. Ingestion Wikimedia → Kafka

**Topics Kafka** :

| Topic | Contenu |
|-------|---------|
| `wm.recentchange.raw` | Tous les événements valides enrichis |
| `wm.bot.events` | Sous-ensemble bots |
| `wm.page.edits` | Sous-ensemble éditions (`edit`) |
| `wm.errors` | JSON invalide / validation échouée |

**Producteur manuel** (flux continu) :

```powershell
pip install -r ingestion/requirements.txt
python ingestion/wikimedia_producer.py --max-events 0
```

**DAG Airflow `wikimedia_ingestion`** : batch toutes les **5 minutes** (300 événements), retry automatique, logs des rejets vers `wm.errors`.

---

## 2. Spark Streaming + Batch

**Streaming** (Kafka → HDFS) :

```powershell
docker exec -it spark-master spark-submit `
  --master spark://spark-master:7077 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 `
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 `
  /opt/bitnami/spark/jobs/wikimedia_stream_consumer.py
```

**Batch** (agrégation processed) :

```powershell
.\scripts\run-spark-batch.ps1
```

**Kafka** : Spark/Docker → `kafka:29092` | Producteur PC → `localhost:9092`

---

## 3. DAGs Airflow et agrégations

```powershell
.\scripts\trigger-all-wikimedia-dags.ps1
```

| DAG | Rôle | Fichiers JSON (exemples) |
|-----|------|---------------------------|
| `wikimedia_ingestion` | Ingestion planifiée → Kafka | — |
| `wikimedia_global_activity` | Activité minute/heure/langue/wiki | `activity_by_hour.json`, `language_distribution.json` |
| `wikimedia_top_pages` | Top pages modifiées/créées/supprimées | `top_pages.json` |
| `wikimedia_user_activity` | Contributeurs, anonymes | `user_activity.json` |
| `wikimedia_bot_activity` | Volume bots, ratio | `bot_ratio.json` |
| `wikimedia_anomaly_detection` | Spike, spam, bots, data quality | `/data/wikimedia/anomalies/` |
| `wikimedia_automated_reporting` | Rapport journalier complet | `/reports/YYYY-MM-DD.json` |
| `wikimedia_pipeline_monitoring` | Statuts DAG, durées, échecs | `monitoring/latest_pipeline_monitoring.json` |
| `wikimedia_hdfs_healthcheck` | Santé HDFS | `hdfs_health_report.json` |

---

## 4. Dashboard Streamlit

http://localhost:8501 — **3 modes** :

| Mode | Source | Contenu |
|------|--------|---------|
| **Historical reports** | HDFS `reports/` | Agrégations batch (graphiques KPI) |
| **Live streaming** | Kafka `wm.recentchange.raw` | events/sec, top pages live, bots live |
| **Monitoring & Ops** | Kafka + Spark UI + Airflow API + HDFS | lag Kafka, jobs Spark, états DAGs, anomalies, taux invalides |

---

## Démo rapide (soutenance)

1. `docker compose up -d` + scripts Kafka/HDFS  
2. Producteur : `python ingestion/wikimedia_producer.py --max-events 500`  
3. Spark streaming + optionnel batch  
4. `.\scripts\trigger-all-wikimedia-dags.ps1`  
5. Dashboard : Historical → Live (producteur `--max-events 0`) → Monitoring & Ops  
6. Vérifier HDFS : http://localhost:9870 → `/data/wikimedia/`

---

## Dépannage

| Problème | Solution |
|----------|----------|
| Spark `localhost:9092` | Utiliser `kafka:29092` dans le consumer |
| DAG ingestion échoue | `docker compose restart airflow-scheduler` ; vérifier volume `./ingestion` monté |
| Aucun rapport HDFS | Lancer prepare-hdfs + Spark + trigger-all DAGs |
| Dashboard ops vide | Exécuter DAGs anomalies + monitoring |

---

## Fonctionnalités livrées

| Composant | Détail |
|-----------|--------|
| Kafka | 4 topics + routage bot / edits / erreurs |
| Ingestion | DAG `wikimedia_ingestion` (batch */5, retry) |
| Data lake | Spark Streaming + batch → HDFS `/data/wikimedia/` |
| Analytics | 5 DAGs → rapports JSON sous `reports/` |
| Qualité | `wikimedia_anomaly_detection` → `anomalies/` |
| Reporting | `wikimedia_automated_reporting` → `YYYY-MM-DD.json` |
| Ops | `wikimedia_pipeline_monitoring` + healthcheck HDFS |
| Dashboard | Streamlit : historique, live Kafka, monitoring |

Ingestion **continue** : `python ingestion/wikimedia_producer.py --max-events 0` (complément au DAG batch planifié).

---

## GitHub

Ne pas committer `.env`. Vérifier `.gitignore` avant push.

```powershell
git add .
git commit -m "Ajout DAG ingestion planifié, anomalies, reporting et monitoring ops"
git push origin main
```

---

Projet pédagogique — respecter les conditions d’utilisation Wikimedia (User-Agent dans le producteur).
