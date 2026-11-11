# Public Libraries Survey Mini-Pipeline

This project uses Airflow, PySpark, and Delta Lake to measure how U.S. public library visits changed from FY2020 to FY2022. The source is the IMLS Public Libraries Survey Administrative Entity (AE) data supplied with the [assignment](TASK.pdf). Each input row describes one library system in one fiscal year.

## Run locally

From the project directory, start Docker Desktop and run these commands in PowerShell.

```powershell
docker compose -p pls-pipeline up -d --build
docker compose -p pls-pipeline ps
```

On the first startup, retrieve the generated password and sign in as `admin`. [Open Airflow](http://127.0.0.1:8082/home).

```powershell
docker compose -p pls-pipeline exec -T airflow cat /opt/airflow/standalone_admin_password.txt
```

Enable and trigger `pls_pipeline`. Leave `fiscal_year` blank to process all three years, or choose `2020`, `2021`, or `2022`. The Airflow Grid shows task status and logs. The separate `pls_smoke_test` DAG verifies that Airflow can submit a Spark job that writes and reads Delta.

Docker Compose runs Airflow with LocalExecutor and a dedicated Postgres metadata database. Airflow launches Spark in local mode through `SparkSubmitOperator`. The project mounts its source data, code, and configuration read-only and stores Delta tables and the Spark catalog in a named volume.

## Pipeline

```text
check_source_files -> bronze_ingest -> silver_clean -> quality_check -> gold_state_kpis
```

| Step | Output and behavior |
|---|---|
| Source check | Verifies that each selected CSV exists and is nonempty. |
| Bronze | `bronze.pls_ae` preserves source columns and flags as strings, adds fiscal year, source filename, and ingestion time, and merges yearly schemas. |
| Silver | `silver.library_year` casts metrics, excludes temporary closures, and keeps one row per `(FSCSKEY, fiscal_year)`. |
| Quality | Checks partition counts, closure counts, nonblank keys, uniqueness, nonnegative metrics, and the accented FY2022 library name for `PR0003`. Failures stop downstream execution. |
| Gold | `gold.state_year_kpis` contains one row per state or territory and year, with library counts, usage totals, per-capita metrics, electronic circulation share, and year-over-year visit change. |

[Source configuration](config/ae_sources.json) defines paths, encodings, and expected counts. FY2020 and FY2022 use Windows-1252, and FY2021 uses UTF-8. Bronze and Silver use `replaceWhere` to replace selected fiscal-year partitions. Gold rebuilds from all available Silver years so a revision also updates the following year's comparison. The DAG allows two retries per task and one active run at a time.

## Data rules and verification

Silver converts `-1`, `-3`, `-4`, and `-9` to null and records the reason per metric in `missing_reason`. It excludes rows with `STATSTRU = 23`. The 13 FY2020 records with `-3` outside that closure status remain in Silver with the affected values set to null.

Gold calculates ratios from state-year totals. Year-over-year visit change requires consecutive years, available visit totals, and a positive previous-year baseline. Puerto Rico first appears in FY2022. The U.S. Virgin Islands have missing visit totals in all three years, so their year-over-year change is also null in FY2021 and FY2022.

Two full DAG runs produced identical row counts. The quality checks passed for all three years.

| Layer | FY2020 | FY2021 | FY2022 | Total |
|---|---:|---:|---:|---:|
| Bronze | 9,245 | 9,215 | 9,248 | 27,708 |
| Silver | 9,241 | 9,207 | 9,238 | 27,686 |
| Gold | 55 | 55 | 56 | 166 |

## Bronze time travel

After Bronze has been populated, run this command from the project directory while the DAG is idle. It shows recent Delta commits and compares row counts by year in version `0` and the latest version. Version `0` reflects the first write in the current Delta volume.

```powershell
docker compose -p pls-pipeline exec -T airflow spark-submit --master 'local[2]' /opt/airflow/jobs/show_bronze_history.py
```

## Findings

The findings rank the five largest increases and five largest declines among the 50 states by percentage change in total visits from FY2020 directly to FY2022. [Findings and reproduction command](FINDINGS.md) · [Visits chart](docs/visits-recovery-fy2020-fy2022.png)

## Project layout

| Path | Contents |
|---|---|
| `dags/` | Main pipeline and smoke test |
| `jobs/` | Spark ingestion, cleaning, validation, aggregation, and ranking |
| `config/` | Source metadata and expected checks |
| `data/` | Supplied survey CSV files |
| `docs/` | Source documentation and findings chart |
