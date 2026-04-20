SHELL := /bin/bash
.ONESHELL:

PROJECT ?= iron-figure-312018
BUCKET  ?= my-speed-lake-$(PROJECT)
RAW     ?= speed_raw
MARTS   ?= speed_marts
LOC     ?= US

# -------------------------------------------------------------------- infra

tf-init:
	cd terraform && terraform init -upgrade

tf-apply:
	cd terraform && terraform apply -auto-approve

tf-destroy:
	cd terraform && terraform destroy -auto-approve

# -------------------------------------------------------------------- airflow

airflow-build:
	cd airflow && docker compose build

airflow-up:
	cd airflow && docker compose up -d

airflow-down:
	cd airflow && docker compose down

airflow-logs:
	cd airflow && docker compose logs -f airflow-scheduler

airflow-nuke:
	cd airflow && docker compose down -v --remove-orphans

trigger-backfill:
	cd airflow && docker compose exec -T airflow-scheduler \
	  airflow dags trigger my_speed_backfill

# -------------------------------------------------------------------- local runs

ingest-one:
	@test -n "$(TYPE)" || (echo "Usage: make ingest-one TYPE=fixed YEAR=2025 Q=1" && exit 1)
	GOOGLE_APPLICATION_CREDENTIALS=$$PWD/credentials/sa-key.json \
	  python3 ingestion/ookla_ingest.py \
	    --type $(TYPE) --year $(YEAR) --quarter $(Q) --bucket $(BUCKET)

dbt-build:
	cd dbt && \
	  GOOGLE_APPLICATION_CREDENTIALS=$$PWD/../credentials/sa-key.json \
	  GCP_PROJECT=$(PROJECT) BQ_MARTS=$(MARTS) BQ_LOCATION=$(LOC) \
	  dbt deps --profiles-dir . && \
	  dbt build --profiles-dir . --target local

# -------------------------------------------------------------------- dashboard

dashboard-install:
	python3 -m pip install --break-system-packages -r dashboard/requirements.txt

dashboard:
	GOOGLE_APPLICATION_CREDENTIALS=$$PWD/credentials/sa-key.json \
	GCP_PROJECT=$(PROJECT) BQ_MARTS=$(MARTS) BQ_LOCATION=$(LOC) \
	  streamlit run dashboard/app.py

# -------------------------------------------------------------------- combos

pipeline-local:
	@echo ">>> ingest 2024Q4 fixed+mobile + 2025Q1 fixed"
	$(MAKE) ingest-one TYPE=fixed  YEAR=2024 Q=4
	$(MAKE) ingest-one TYPE=mobile YEAR=2024 Q=4
	$(MAKE) ingest-one TYPE=fixed  YEAR=2025 Q=1
	@echo ">>> refresh external table"
	python3 -c "from scripts.refresh_ext_table import main; main()"
	@echo ">>> dbt"
	$(MAKE) dbt-build

.PHONY: tf-init tf-apply tf-destroy \
        airflow-build airflow-up airflow-down airflow-logs airflow-nuke trigger-backfill \
        ingest-one dbt-build dashboard-install dashboard pipeline-local
