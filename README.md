# Wikimedia Big Data — Pipeline temps réel et analytique

Projet de **Master Big Data** : ingestion du flux public **Wikimedia RecentChange**, traitement **Kafka + Spark Structured Streaming**, stockage **HDFS**, orchestration **Airflow 3**, visualisation **Streamlit** (rapports historiques et flux live).

---

## Objectif du projet

Construire un pipeline Big Data complet, reproductible en local avec Docker :

1. **Ingérer** des événements Wikimedia en temps réel (SSE → Kafka).
2. **Transformer et persister** le flux avec Spark vers un Data Lake HDFS.
3. **Produire des rapports batch** avec Airflow (JSON analytiques sur HDFS).
4. **Visualiser** les résultats dans un dashboard (mode historique + mode live Kafka).

---

## Architecture

```text
                    ┌─────────────────────────────────────────┐
                    │         Wikimedia EventStream (SSE)      │
                    └────────────────────┬────────────────────┘
                                         │
                                         ▼
                              ┌──────────────────┐
                              │ Producteur Python │
                              └────────┬─────────┘
                                       │
                                       ▼
                         Kafka  topic: wm.recentchange.raw
                         (localhost:9092  |  kafka:29092)
                                       │
              ┌────────────────────────┼────────────────────────┐
              │                        │                        │
              ▼                        ▼                        ▼
    Spark Structured          Dashboard Live              wm.errors
    Streaming (HDFS)               mode                  (événements invalides)
              │
              ▼
    HDFS  /hdfs-data/wikimedia/processed/
              │
              ▼
    Airflow 3  —  5 DAGs  —  WebHDFS
              │
              ▼
    HDFS  /hdfs-data/wikimedia/reports/
              │
              ▼
    Streamlit  —  mode Historical reports
```

---

## Stack technique

| Composant | Technologie | Version |
|-----------|-------------|---------|
| Messaging | Apache Kafka (Confluent) | 7.5.3 |
| Stream processing | Spark Structured Streaming | 3.5.1 |
| Stockage | HDFS (Hadoop 3.2) | images bde2020 |
| Orchestration | Apache Airflow | 3.2.1 |
| Ingestion | Python (`requests`, `sseclient-py`, `kafka-python`) | 3.11+ |
| Visualisation | Streamlit, Plotly, Pandas | voir `dashboard/requirements.txt` |
| Conteneurisation | Docker Compose | v2 |

**Airflow 3** : interface via `airflow-api-server` (port hôte **8088**), pas de `webserver` comme en v2.

---

## Structure du projet

```text
.
├── docker-compose.yml       # Stack complète
├── .env.example             # Modèle de secrets (copier vers .env)
├── config/hadoop.env        # Configuration HDFS
├── ingestion/
│   ├── wikimedia_producer.py
│   └── requirements.txt
├── spark/jobs/
│   └── wikimedia_stream_consumer.py
├── airflow/dags/
│   ├── common/wikimedia_hdfs_utils.py
│   ├── wikimedia_hdfs_healthcheck_dag.py
│   ├── wikimedia_global_activity_dag.py
│   ├── wikimedia_top_pages_dag.py
│   ├── wikimedia_user_activity_dag.py
│   ├── wikimedia_bot_activity_dag.py
│   └── hello_world.py
├── dashboard/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── scripts/                 # Topics Kafka, HDFS, DAGs, smoke tests
└── docs/screenshots/        # Captures optionnelles pour le README
```

---

## Installation

### Prérequis

- Docker Desktop (Windows/macOS) ou Docker Engine + Compose v2.14+
- 8 Go RAM minimum pour Docker (16 Go recommandé)
- Python 3.11+ sur la machine hôte (producteur uniquement)
- Ports libres : `2181`, `7077`, `8020`, `8080`, `8081`, `8088`, `8501`, `9092`, `9864`, `9870`

### Configuration

```powershell
git clone <url-du-repo>
cd Projet_Wikimedia_HD
copy .env.example .env
```

Éditer `.env` : générer `AIRFLOW_FERNET_KEY` et changer les mots de passe.

```powershell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

```powershell
pip install -r ingestion/requirements.txt
```

---

## Lancement avec Docker

```powershell
docker compose pull
docker compose up -d
docker compose ps
```

Préparation des topics et dossiers HDFS :

```powershell
.\scripts\create-kafka-topics.ps1
.\scripts\prepare-hdfs-wikimedia.ps1
docker compose build dashboard
docker compose up -d dashboard
```

Smoke test :

```powershell
.\scripts\test-services.ps1
```

### Interfaces web

| Service | URL | Port |
|---------|-----|------|
| HDFS Namenode | http://localhost:9870 | 9870 |
| HDFS Datanode | http://localhost:9864 | 9864 |
| Spark Master | http://localhost:8080 | 8080 |
| Airflow 3 | http://localhost:8088 | 8088 |
| Dashboard Streamlit | http://localhost:8501 | 8501 |

Connexion Airflow : **admin** / valeur de `AIRFLOW_ADMIN_PASSWORD` dans `.env`.

### Kafka — quelle adresse utiliser ?

| Client | `bootstrap.servers` |
|--------|---------------------|
| Producteur Python (PC hôte) | `localhost:9092` |
| Spark / Dashboard (Docker) | `kafka:29092` |

Ne pas utiliser `kafka:9092` depuis un conteneur : Kafka annonce `localhost:9092` dans les métadonnées et les workers ne peuvent pas s’y connecter.

---

## Ingestion Wikimedia vers Kafka

Écoute du flux SSE Wikimedia et publication sur Kafka.

```powershell
python ingestion/wikimedia_producer.py --max-events 500
```

Flux continu (pour le dashboard live) :

```powershell
python ingestion/wikimedia_producer.py --max-events 0
```

| Topic | Rôle |
|-------|------|
| `wm.recentchange.raw` | Événements valides enrichis |
| `wm.errors` | JSON invalide ou champs manquants |

Vérification :

```powershell
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic wm.recentchange.raw --from-beginning --max-messages 3
```

---

## Traitement Spark Structured Streaming

Job : `spark/jobs/wikimedia_stream_consumer.py` — Kafka → console (debug) + JSON sur HDFS.

```powershell
docker exec -it spark-master spark-submit `
  --master spark://spark-master:7077 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 `
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 `
  /opt/bitnami/spark/jobs/wikimedia_stream_consumer.py
```

| Chemin HDFS | Rôle |
|-------------|------|
| `/hdfs-data/wikimedia/processed/` | Data Lake (JSON ligne par ligne) |
| `/hdfs-data/wikimedia/checkpoints/stream-consumer/` | Checkpoints Spark (reprise du job) |

Explorateur HDFS : http://localhost:9870/explorer.html#/hdfs-data/wikimedia/processed

---

## Stockage HDFS

Arborescence créée par `scripts/prepare-hdfs-wikimedia.ps1` :

| Chemin | Contenu |
|--------|---------|
| `/hdfs-data/wikimedia/processed/` | Événements transformés (Spark) |
| `/hdfs-data/wikimedia/checkpoints/` | Checkpoints streaming |
| `/hdfs-data/wikimedia/reports/` | Rapports JSON (Airflow) |

RPC namenode : `hdfs://namenode:8020` (voir `config/hadoop.env`).

---

## DAGs Airflow

Cinq DAGs analytiques + un DAG de test `hello_world`.

```powershell
.\scripts\trigger-all-wikimedia-dags.ps1
```

Les DAGs sont **en pause par défaut** : le script les active et les déclenche.

| DAG | Rapports produits |
|-----|-------------------|
| `wikimedia_hdfs_healthcheck` | `reports/hdfs_health_report.json` |
| `wikimedia_global_activity` | `reports/global/*.json` |
| `wikimedia_top_pages` | `reports/top_pages/*.json` |
| `wikimedia_user_activity` | `reports/users/*.json` |
| `wikimedia_bot_activity` | `reports/bots/*.json` |

UI : http://localhost:8088

---

## Dashboard Streamlit

http://localhost:8501

### Mode historique

- Source : rapports JSON sur HDFS (`/hdfs-data/wikimedia/reports/`) via WebHDFS.
- Générés par les DAGs Airflow (batch).
- Rafraîchissement automatique ~30 s.
- KPIs, graphiques (activité, pages, utilisateurs, bots), fraîcheur des rapports.

### Mode live Kafka

- Source : topic `wm.recentchange.raw` (consommation directe Kafka).
- Nécessite le producteur actif (`--max-events 0`).
- Tampon session (1000 événements max), poll non bloquant, refresh ~10 s.
- Options sidebar : *nouveaux messages uniquement* (recommandé) ou *depuis le début* (debug).
- Panneau **Debug Kafka** : dernier poll, consumer group, horodatages UTC/Paris.

| Mode | Type | Stockage |
|------|------|----------|
| Historical | Batch | HDFS `reports/` |
| Live | Temps réel | Kafka (tampon mémoire Streamlit) |

---

## Démo rapide

1. `docker compose up -d`
2. `.\scripts\create-kafka-topics.ps1` et `.\scripts\prepare-hdfs-wikimedia.ps1`
3. Producteur : `python ingestion/wikimedia_producer.py --max-events 500`
4. Spark streaming : `spark-submit` (commande ci-dessus)
5. DAGs : `.\scripts\trigger-all-wikimedia-dags.ps1` — attendre les runs verts
6. HDFS : http://localhost:9870 → `processed/` et `reports/`
7. Airflow : http://localhost:8088
8. Dashboard : Historical puis Live (producteur en continu pour le live)

---

## Dépannage

| Problème | Solution |
|----------|----------|
| `ModuleNotFoundError: sseclient` | `pip install -r ingestion/requirements.txt` |
| Spark : `Connection to localhost:9092` | Consumer Spark : `kafka:29092` |
| Aucun fichier dans `processed/` | Producteur + job Spark actifs ; attendre 10–30 s |
| Dashboard : aucun rapport | `trigger-all-wikimedia-dags.ps1`, attendre success |
| Live : aucun événement | Producteur `--max-events 0` ; **Vider tampon live** |
| Live : horodatages bloqués | **Vider tampon live** + relancer le producteur |
| Airflow DAGs absents | `docker compose logs airflow-dag-processor` |
| HDFS datanode non enregistré | `docker compose restart datanode` |

---

## Limites et améliorations possibles

**Limites actuelles**

- Stack locale Docker, non adaptée à la production (sécurité, HA, quotas).
- JSON sur HDFS (verbose) ; pas de partitionnement Parquet/Delta.
- Deux lectures Kafka parallèles dans Spark (console + HDFS) pour respecter une sink par flux.
- Dashboard live : tampon en mémoire Streamlit, pas de persistance long terme.
- Pas de authentification Kafka/HDFS en dehors des secrets Airflow.

**Pistes d’évolution**

- Écrire en **Parquet** partitionné par date/wiki depuis Spark.
- Fusionner console + HDFS en une seule requête avec `foreachBatch`.
- Alerting (seuil bots, pages sensibles) via Airflow + notifications.
- Déploiement sur VM cloud avec Compose ou Kubernetes (Strimzi, etc.).
- Schéma Registry Kafka pour contrôler l’évolution des messages.

---

## GitHub

Avant le push :

- Ne **jamais** committer `.env` (utiliser `.env.example`)
- Vérifier que `airflow/logs/`, `.venv/`, exports JSON locaux sont ignorés
- Pas de mots de passe réels dans les captures d’écran

```powershell
git status
git add .
git commit -m "Projet Wikimedia Big Data — pipeline Kafka, Spark, HDFS, Airflow, Streamlit"
git remote add origin <url-du-repo>
git push -u origin main
```

---

## Licence et usage

Projet pédagogique — flux public Wikimedia ([EventStreams](https://stream.wikimedia.org/)).  
Respecter les conditions d’utilisation Wikimedia (User-Agent descriptif dans le producteur).
