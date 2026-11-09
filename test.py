from diagrams import Diagram, Cluster
from diagrams.aws.compute import Lambda, EC2
from diagrams.aws.integration import SQS
from diagrams.aws.storage import S3
from diagrams.aws.analytics import Athena, GlueDataCatalog
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.analytics import Dbt

with Diagram("Movie Data Lakehouse Architecture", show=False, direction="LR"):
    airflow = Airflow("Airflow Orchestrator\n(on EC2)")

    with Cluster("Daily Snapshot Pipeline"):
        snap_lambda = Lambda("Snapshot Lambda")
        raw_snap = S3("raw/daily_snapshot\n(NDJSON)")
        airflow >> snap_lambda >> raw_snap

    with Cluster("Daily Changes Pipeline (Producer-Consumer)"):
        prod_lambda = Lambda("Producer Lambda")
        sqs = SQS("Task Queue")
        worker_lambda = Lambda("Worker Pool")
        raw_changes = S3("raw/daily_changes\n(NDJSON)")

        airflow >> prod_lambda >> sqs >> worker_lambda >> raw_changes

    with Cluster("Transformation & Lakehouse Marts"):
        dbt = Dbt("dbt-athena")
        athena = Athena("Athena Engine")
        glue = GlueDataCatalog("Glue Catalog")
        gold_s3 = S3("gold/ (Parquet / Marts)")

        airflow >> dbt >> athena
        athena - glue
        athena >> gold_s3
