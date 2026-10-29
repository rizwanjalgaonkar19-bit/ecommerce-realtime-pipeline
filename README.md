# Real-Time E-Commerce Data Engineering Pipeline

B.Tech CSE (7th semester) major project.
A local, replay-based streaming pipeline: dataset → Kafka → validation and quarantine → Spark Structured Streaming → Parquet → SQL analytics → Streamlit dashboard, plus anomaly detection on the processed data.

## Data sources

| Stream | Source | Records |
|---|---|---|
| orders | Olist Brazilian E-Commerce Public Dataset (Kaggle), real and anonymized | 102,425 |
| customers | same dataset | 99,441 |
| products | same dataset | 32,951 |
| clickstream | **synthetic**, made by `src/generator.py` | 21,000 |

- This is **not** Flipkart data. It is a Brazilian marketplace dataset.
- Olist has no clickstream, so those events are synthetic (about 70% view, 20% add_to_cart, 10% purchase, set by the generator). They show that the pipeline works, not real customer behaviour.
- The data is historical, so it is **replayed** into Kafka. It is not a live feed.
- `quantity` and `total_amount` are not in the original files. They are derived (items per order and product, unit price x quantity). Fields such as customer name and product price are not in the dataset, so they are not in the schema.
- Dataset: Kaggle, "Brazilian E-Commerce Public Dataset by Olist". License: see the dataset page on Kaggle.

## Architecture

```
Olist CSV files + synthetic clicks
        |
   producer.py  (replay)
        v
   Kafka topics: orders | customers | products | clickstream
        |                                   |
        v                                   v
 stream_validator.py                 spark_streaming.py
 (schema, types, nulls, rules,       (clean, add time columns)
  timestamps, duplicates)                   |
   |            |                           v
 valid      invalid ──> data/quarantine   Parquet: raw + processed
 (metrics)     (JSON Lines)                 |
                                            v
                                  DuckDB SQL ──> Streamlit dashboard
                                            |
                                            v
                                  Isolation Forest (anomaly detection)
```

## Folder structure

```
config/        settings (empty for now)
data/raw/      Olist CSV files (not in git)
data/quarantine/   invalid records, one .jsonl file per stream
results/       saved outputs of tests and experiments
sql/           kpi_queries.sql
src/           schemas, loader, generator, producer, consumer, validator, quarantine,
               metrics, stream_validator, spark_streaming, analytics, dashboard,
               fault_test, anomaly_detector, benchmark, spark_bench
tests/         test_validator.py
```

Spark output goes to `C:/pipeline-data/` (short path, outside the project folder).

## Setup (Windows)

- JDK 17, Python 3.11, Git
- Kafka 3.9.1 extracted to `C:\kafka` (KRaft mode, one broker)
- `winutils.exe` and `hadoop.dll` (Hadoop 3.3.5) in `C:\hadoop\bin`
- Windows long paths enabled
- Olist CSV files copied into `data/raw/`

```
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

## How to run

Start Kafka in its own terminal:

```
C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\kraft\server.properties
```

Create the topics once: `orders`, `customers`, `products`, `clickstream` (1 partition, replication factor 1). Then, from the project folder:

```
python -m tests.test_validator      # unit tests for the validator
python -m src.producer              # replay data into Kafka
python -m src.stream_validator      # validate, quarantine, print data-quality metrics
python -m src.spark_streaming       # write raw and processed Parquet
python -m src.analytics             # run the SQL KPIs
streamlit run src/dashboard.py      # dashboard at localhost:8501
python -m src.fault_test            # fault injection test
python -m src.anomaly_detector      # Isolation Forest experiment
python -m src.benchmark             # benchmark (needs Kafka running, charger plugged in)
```
## Run with Docker

Needs Docker Desktop and the Olist CSV files in `data/raw/` (not in git).

```
docker compose up --build
```

This starts Kafka, replays the dataset (producer), runs the validator and Spark Streaming, and then starts the dashboard at http://localhost:8501. Invalid records are written to `data/quarantine/`. Spark output stays in a Docker volume (`pipeline-data`). To remove everything: `docker compose down -v`. Delete `data/quarantine/*.jsonl` before a fresh run, otherwise the quarantine lines are written twice.

Result of one Docker run (logs in `results/docker_*_log.txt`): the producer sent 255,817 records, the validator found 255,205 valid and 612 invalid (99.76%), Spark finished with exit code 0, and the dashboard showed the same numbers as the local run (98,204 orders, 13,496,273 BRL). Consume + validate speed in this single run was 9,678 records/s.

## Run on Kubernetes (Docker Desktop)

Needs Docker Desktop with Kubernetes enabled, the image built with `docker build -t ecommerce-pipeline .`, and the five Olist CSV files copied to `C:\pipeline-raw` (the producer job reads them through a `hostPath` volume). Manifests are in `k8s/`.

```
kubectl apply -f k8s\00-storage.yaml -f k8s\01-kafka.yaml
kubectl rollout status deployment/kafka --timeout=180s
kubectl apply -f k8s\02-producer-job.yaml
kubectl wait --for=condition=complete job/producer --timeout=600s
kubectl apply -f k8s\03-validator-job.yaml -f k8s\04-spark-job.yaml
kubectl wait --for=condition=complete job/validator job/spark --timeout=900s
kubectl apply -f k8s\05-dashboard.yaml
kubectl port-forward service/dashboard 8501:8501
```

Kafka and the dashboard are Deployments. The producer, validator and Spark steps are Jobs. Kafka has its own volume (`kafka-data`), and the validator, Spark and dashboard share another one (`pipeline-data`). Saved logs are in `results/k8s_*`.

Result of one run: the validator printed the same numbers as the local and Docker runs (255,817 records, 255,205 valid, 612 invalid, 99.76%). The Job durations were 24 s (producer), 26 s (validator) and 44 s (Spark). These are single-run times, not benchmarks.

Failure tests (`results/k8s_failure_test.txt`):

- Deleting the dashboard pod: Kubernetes created a new pod in about 2 seconds and it showed `1/1 Running` after 22 seconds. No readiness check was configured, so this does not prove the page was serving.
- Deleting the Kafka pod while Kafka had no volume: the topics and all data were lost. The Spark Parquet files on the other volume stayed.
- After adding the `kafka-data` volume, the `orders` offset was 102,425 before and after deleting the Kafka pod, and all four topics were still listed. Only the `orders` offset and the topic list were checked.
- The first version of this test used a wrong Kafka command (`kafka.tools.GetOffsetShell`), so those lines in the file show errors. The test was repeated with `kafka-get-offsets.sh`.
- Job logs are kept only inside the cluster. After Docker Desktop restarted, the logs of finished Jobs were gone, so they were saved to files right after each run.

Limits: one node, volumes stored on the laptop disk (they protect against a pod failing, not against the laptop failing), no authentication or encryption, no resource limits, and the image is built locally (`imagePullPolicy: Never`).

## Results (measured on the author's laptop)

Laptop: ASUS Vivobook 16X, Intel Core i5-13420H, about 16 GB RAM, Windows.
Software: Python 3.11.9, JDK 17, Kafka 3.9.1 (single broker), Spark 3.5.1 (local mode, 2 cores).

**Data quality, full dataset (255,817 records)**

- Valid: 255,205. Invalid: 612 (99.76% valid). Duplicates: 0.
- All 612 invalid records had missing values: 611 products (missing category or weight) and 1 order (missing payment method). They are in `data/quarantine/`.
- Spark's processed output has 255,205 records, the same as the validator's valid count (orders 102,424, customers 99,441, products 32,340, clickstream 21,000).
- One full Spark run for all four streams (raw and processed) took 38.1 s wall-clock, including Spark start-up.
- The original data is quite clean, so the validation rules were also tested with injected faults (below).

**Fault injection test** (`results/fault_test_output.txt`)

999 clean orders plus 140 injected faulty orders (20 each of: duplicate, negative quantity, bad timestamp, missing field, wrong type, wrong total, invalid status). All 140 were caught, and no clean record was rejected (validation rate 87.71%, expected because the faults were injected on purpose).

**Anomaly detection** (Isolation Forest on processed orders; `results/anomaly_output.txt`)

The dataset has no fraud labels, so evaluation uses **injected (synthetic) anomalies**, 1,000 out of 102,424 orders.

| Test | Precision | Recall | F1 |
|---|---|---|---|
| Strong (price x10-50, quantity 5-30) | 0.93 | 0.93 | 0.93 |
| Mild (price x3-6) | 0.10 | 0.10 | 0.10 |

The model finds large, obvious outliers but is weak on mild ones. Precision and recall are equal because the contamination setting was set to the exact number of injected anomalies.

**Benchmark** (10,000 / 50,000 / 100,000 orders, 3 runs each; `results/benchmark_results.csv`)

- In all 9 runs, consumed = size, valid = size - 1, and Spark rows = valid. Nothing was lost or duplicated between Kafka, the validator and Spark.
- Speed varied a lot between runs (for example, consume + validate at 100,000 orders: 6,575 to 20,661 records/s), so only ranges are reported.
- Spark job time was 7.5 to 23.2 s for all sizes, so at this scale it is dominated by fixed overhead, not data volume. These numbers do not show how Spark scales.

**Benchmark v2** (one warm-up run, then 5 runs each for 10,000, 50,000 and 102,425 orders; `results/benchmark_results_v2.csv`)

- In all 15 runs, consumed = size, valid = size - 1 and Spark rows = valid, so nothing was lost or duplicated.
- Median producer speed was 10,365 / 12,155 / 11,476 records/s and median consume + validate speed was 18,258 / 16,589 / 16,891 records/s (10,000 / 50,000 / 102,425 orders). The slowest runs were about 4 times slower than the fastest, and in the three slowest runs the producer and the validator were both slow, so something affecting the whole machine or Kafka at that moment is likely, but this was not tested.
- Median Spark job time was 7.0 / 8.1 / 12.1 s (min to max: 6.5-15.1, 7.4-9.9, 8.7-21.0 s). The ranges overlap and each size has only 5 runs, so this does not show how Spark scales. The first two runs at 10,000 orders were slower than the later ones even after the warm-up run.

## Security notes

- No passwords, tokens or keys are stored in the code. Settings (Kafka address, data folders) come from environment variables (`config/settings.py`).
- `.gitignore` and `.dockerignore` keep the virtual environment, the dataset, quarantine files, checkpoints and logs out of git and out of the Docker image.
- The tracked files were scanned with `git grep` for password, secret, token and key words. Only the name of a Spark library matched.
- Kafka runs without authentication or encryption. This is fine for a single local machine, but a real deployment would need TLS, SASL logins and access rules.
- The Docker image does not create a non-root user, so the containers run as root. A production image should not do this.
- The dataset is anonymized, but the license on Kaggle still applies.

## Limitations

- One machine, one Kafka broker, one partition, Spark in local mode. Results do not describe cluster performance.
- Replay of historical data, not a live feed.
- Clickstream is synthetic.
- Anomaly detection is evaluated only on injected anomalies.
- Benchmark has only 3 runs per size and high variance.

## Limitations and future work

- **GitHub Actions:** the workflow in `.github/workflows/tests.yml` is written but has not run yet, because the code has not been pushed to GitHub.
- **Real data and labels:** the data is replayed history, the clickstream is synthetic, and there are no fraud labels, so the anomaly detection result only shows that injected outliers can be found.
- **Scale:** one Kafka broker with one partition, and Spark in local mode. Running with more partitions and on a multi-node cluster was not tested.
