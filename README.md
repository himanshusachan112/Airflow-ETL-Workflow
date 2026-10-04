# Airflow ETL Workflow

====================================STEP1===============================================
Local Apache Airflow 3.0 setup via Docker Compose, using `LocalExecutor` with PostgreSQL as the metadata DB.

## Services
- **postgres** – metadata database
- **airflow-apiserver** – web UI/API (port `8080`)
- **airflow-scheduler** – schedules DAGs
- **airflow-dag-processor** – parses DAG files
- **airflow-triggerer** – handles deferred tasks
- **airflow-init** – one-time DB migration + admin user setup
- **airflow-cli** – optional debug CLI (`--profile debug`)

## Usage
```bash
docker-compose up airflow-init
docker-compose up
```

UI: `http://localhost:8080` (login: `airflow` / `airflow`)

DAGs, logs, plugins, and config are mounted from local `dags/`, `logs/`, `plugins/`, `config/` folders.

=============================================END======================================



## User Processing DAG
The `user_processing.py` DAG creates a PostgreSQL `users` table, then checks a public fake-user JSON endpoint every 30 seconds (for up to 5 minutes). When the endpoint responds successfully, it extracts the user's ID, first name, last name, and email, writes them to `/tmp/user_info.csv`, and loads the CSV row into PostgreSQL using `PostgresHook`.

==================================================Asset===================================

## User Assets
The `user.py` file defines a daily `user` asset that fetches JSON data from `randomuser.me/api/`. A downstream multi-asset, scheduled by `user`, reads the fetched data and materializes two assets: `user_location` and `user_login`.




