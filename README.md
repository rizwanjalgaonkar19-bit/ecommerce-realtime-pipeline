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

## Limitations

- One machine, one Kafka broker, one partition, Spark in local mode. Results do not describe cluster performance.
- Replay of historical data, not a live feed.
- Clickstream is synthetic.
- Anomaly detection is evaluated only on injected anomalies.
- Benchmark has only 3 runs per size and high variance.

## Pending

Docker, Kubernetes, AWS S3 and CI/CD are not done yet. This section will be updated.

## AI assistance

Claude (Anthropic) was used for guidance and for drafting code. All code was run and tested by the author on their own laptop.