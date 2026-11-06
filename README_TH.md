## 6. ELT Performance & Cost Optimization

| Optimization Technique | Layer | Implementation Details | Impact / Benefit |
|---|---|---|---|
| **Hive-style Partitioning** | Storage / Ingestion | Partitioned S3 paths (`year/month/day`) registered via Glue Catalog | Enables **Partition Pruning** in Athena, slashing query scan sizes and cloud costs by up to 90%. |
| **Stream-friendly NDJSON** | Ingestion Layer | Converted snapshot payloads to Newline Delimited JSON | Eliminates full-array JSON memory parsing overhead on Athena query workers. |
| **Pushdown Deduplication** | Transformation (dbt) | Used `ROW_NUMBER() OVER (...)` CTEs in Staging models | Avoids costly row-level file rewriting on S3; pushes heavy dedup logic to Athena distributed SQL engine. |
| **Analytical Pre-aggregation** | Data Modeling | Materialized TF-IDF scores into dedicated fact tables (`fact_movie_keyword_tfidf`) | Eliminates heavy multi-pass string aggregations on runtime dashboard queries. |
| **Normalized Bridge Tables** | Data Modeling | Split M:N relationships into surrogate-keyed bridge tables | Avoids high-latency `CROSS JOIN UNNEST()` operations over complex nested JSON arrays in Athena. |
| **OS Memory Swap Tuning** | Orchestration | Provisioned 4GB swap space on host EC2 | Prevents Linux OOM Killer terminations during Airflow scheduler/webserver memory spikes. |