# End-to-End Movie Data Lakehouse & Automated Orchestration | Souce: TMDB

![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Lambda%20%7C%20SQS%20%7C%20Athena%20%7C%20Glue%20%7C%20EC2-232F3E?logo=amazon-aws)
![Apache Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE?logo=apache-airflow)
![dbt](https://img.shields.io/badge/Transformation-dbt--core-FF694B?logo=dbt)
![Docker](https://img.shields.io/badge/Container-Docker-2496ED?logo=docker)
![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-2088FF?logo=github-actions)

## Table of Contents

- [🇹🇭 Thai Version](#th-version)
  - [1. ที่มาและเป้าหมาย](#1-ที่มาและเป้าหมาย-motivation--objective)
  - [2. แผนภาพสถาปัตยกรรมระบบ](#2-แผนภาพสถาปัตยกรรมระบบ-architecture-flow)
  - [3. ปัญหาทางเทคนิคและการตัดสินใจเชิงวิศวกรรม](#3-ปัญหาทางเทคนิคและการตัดสินใจเชิงวิศวกรรม-technical-challenges--trade-offs)
  - [4. ตาราง-elt-optimization-matrix](#4-ตาราง-elt-optimization-matrix)
  - [5. แบบจำลองข้อมูลระดับ Marts](#5-แบบจำลองข้อมูลระดับ-marts-dimensional-data-model)
  - [6. การนำส่งโค้ดอัตโนมัติ](#6-การนำส่งโค้ดอัตโนมัติ-cicd-workflows)
  - [7. โครงสร้างโปรเจกต์](#7-โครงสร้างโปรเจกต์-repository-structure)
  - [8. วิธีการติดตั้งและรันระบบ](#8-วิธีการติดตั้งและรันระบบ-setup--configuration)

- [en English Version](#en-version)
  - [1. Motivation & Objectives](#1-motivation--objectives)
  - [2. System Architecture Flow](#2-system-architecture-flow)
  - [3. Technical Challenges & Engineering Trade-offs](#3-technical-challenges--engineering-trade-offs)
  - [4. ELT Optimization Matrix](#4-elt-optimization-matrix)
  - [5. Dimensional Data Model (Marts)](#5-dimensional-data-model-marts)
  - [6. Automated Deployment (CI/CD Workflows)](#6-automated-deployment-cicd-workflows)
  - [7. Repository Structure](#7-repository-structure)
  - [8. Setup & Configuration](#8-setup--configuration)

## TH-Version

ระบบ Data Lakehouse แบบ Event-Driven ELT Pipeline บนสถาปัตยกรรม Serverless และ Container ดึงข้อมูลภาพยนตร์จาก TMDB API จัดเก็บลง AWS S3 แปลงข้อมูลสู่ Dimensional Model (Star Schema) บน AWS Athena ด้วย dbt Core ควบคุมการทำงานอัตโนมัติด้วย Apache Airflow บน AWS EC2 พร้อมระบบ Continuous Deployment (CD) ผ่าน GitHub Actions

---

## 1. ที่มาและเป้าหมาย (Motivation & Objective)

ระบบ Automated Data Lakehouse ที่พัฒนาขึ้นโดยใช้สถาปัตยกรรมแบบ Event-Driven และ ELT Pipeline สำหรับดึงข้อมูล แปลงข้อมูล และสร้างแบบจำลองข้อมูลภาพยนตร์จาก TMDB API เพื่อรองรับการวิเคราะห์ข้อมูลใน 3 มิติหลัก ได้แก่ การวิเคราะห์ผลตอบแทนทางการเงิน (Financial ROI) การติดตามแนวโน้มความนิยมของภาพยนตร์ในช่วง 30 วันแรกหลังเข้าฉาย (30-Day Theatrical Trajectory) และการวิเคราะห์ประเด็นเนื้อหาที่โดดเด่นผ่านการคำนวณ TF-IDF ในระดับ Keyword (TF-IDF Keyword Analysis)

ระบบออกแบบตามแนวคิด Decoupled Storage & Compute บน AWS โดยใช้ Amazon S3 เป็นพื้นที่จัดเก็บข้อมูล, AWS Glue Data Catalog สำหรับจัดการ Metadata, Amazon Athena สำหรับการ Query ข้อมูล และ dbt Core สำหรับการแปลงและสร้างแบบจำลองข้อมูล พร้อมจัดการลำดับการทำงานของ Pipeline ด้วย Apache Airflow และใช้ GitHub Actions สำหรับทำ Continuous Deployment (CD) โดยอัตโนมัติ

* **ปัญหาทางธุรกิจ:** การลงทุนในอุตสาหกรรมภาพยนตร์มีความเสี่ยงสูง การตัดสินใจลงทุนหรือพัฒนาโครงการภาพยนตร์จึงจำเป็นต้องอาศัยข้อมูลประกอบมากกว่าการพิจารณาจากสัญชาตญาณหรือรายได้รวมหลังภาพยนตร์ออกฉายจบเพียงอย่างเดียว โดยยังมีประเด็นสำคัญที่สามารถนำข้อมูลมาวิเคราะห์เพื่อสนับสนุนการตัดสินใจได้ ได้แก่
- แนวโน้มความนิยม: ขาดเครื่องมือที่ช่วยติดตามและเปรียบเทียบแนวโน้มความนิยมของภาพยนตร์ในช่วง 30 วันแรกหลังเข้าฉาย เพื่อให้เห็นทิศทางความนิยมในช่วงเวลาสำคัญของการฉาย
- ประเด็นเนื้อหาภาพยนตร์: ขาดมุมมองในการวิเคราะห์ว่าในแต่ละปี ประเภทภาพยนตร์ หรือประเทศ มีประเด็นและ Keyword ใดที่โดดเด่นหรือถูกนำมาใช้บ่อย ซึ่งอาจช่วยให้ผู้ผลิตมองเห็นแนวโน้มของเนื้อหาที่ได้รับความนิยม รวมถึงค้นหาแนวทางสร้างสรรค์เนื้อหาที่แตกต่างจากสิ่งที่มีอยู่ในตลาด
* **เป้าหมายเชิงวิศวกรรม:** ออกแบบและพัฒนา Lakehouse Architecture ที่มีความเสถียร รองรับการขยายตัว (Scalable) และสามารถควบคุมต้นทุนการประมวลผลบนคลาวด์ได้อย่างเหมาะสม โดยใช้สถาปัตยกรรมแบบ Decoupled Storage & Compute เพื่อแยกส่วนการจัดเก็บข้อมูลและการประมวลผลออกจากกัน พร้อมพัฒนา ELT Pipeline ที่ทำงานโดยอัตโนมัติผ่าน Apache Airflow และ GitHub Actions เพื่อจัดเตรียมข้อมูลให้อยู่ในรูปแบบพร้อมใช้งานสำหรับการวิเคราะห์ (Analytics-Ready Data) และนำไปแสดงผลผ่าน BI Dashboard เพื่อสนับสนุนการวิเคราะห์ผลตอบแทนทางการเงิน แนวโน้มความนิยมของภาพยนตร์ และการค้นหาประเด็นเนื้อหาที่โดดเด่นซึ่งสามารถนำไปต่อยอดเป็นแนวคิดในการพัฒนาภาพยนตร์เรื่องใหม่ได้

---

## 2. แผนภาพสถาปัตยกรรมระบบ (Architecture Flow)

```plaintext
[Airflow DAG: movie_data_pipeline_orchestration]
  │
  ├─ 1. invoke_snapshot_lambda ──> [AWS Lambda: Snapshot]
  │                                       │ (TMDB Discover API: แบ่งช่วง 0-14 และ 15-30 วัน)
  │                                       ▼
  │                                [S3: raw/movie_daily_snapshot/...] (รูปแบบ NDJSON เท่านั้น)
  │
  ├─ 2. invoke_producer_lambda ──> [AWS Lambda: Producer]
  │                                       │ (TMDB Changes API: /movie/changes)
  │                                       ▼
  │                                [Amazon SQS Queue (Task Distribution Buffer)]
  │                                       │
  │                                       ▼ (Concurrency Scaling)
  │                                [AWS Lambda: Worker Pool]
  │                                       │ (TMDB Detail API: /movie/{id})
  │                                       ▼
  │                                [S3: raw/movie_daily_changes/...] (NDJSON เท่านั้น)
  │
  ├─ 3. wait_for_workers_completion (Time Delay 7 นาที เพื่อรอ Worker ประมวลผลครบ)
  │
  └─ 4. trigger_dbt_daily_models ──> [dbt-athena บน Airflow/EC2]
                                           │
                                           ▼ (Dimensional Modeling: Staging -> Intermediate -> Marts)
                                    [AWS Athena + AWS Glue Catalog]
                                           │
                                           ▼
                                    [S3 Target Storage: gold/ (Parquet / Marts)]
```

---

## 3. ปัญหาทางเทคนิคและการตัดสินใจเชิงวิศวกรรม (Technical Challenges & Trade-offs)

| Engineering Challenge | The Bottleneck / Failure Mode | Architectural Solution | Trade-off & Mitigation |
|---|---|---|---|
| **500-Page API Limit** | Endpoint `/discover/movie` จำกัดการดึงสูงสุด 500 หน้า เสี่ยงเกิด Data Loss ช่วงที่มีภาพยนตร์เข้าฉายพร้อมกันจำนวนมาก | **Window Splitting Strategy:** ซอยช่วงเวลาค้นหาเป็น 2 กรอบ: `0–14 วัน` และ `15–30 วัน` แล้วนำมารวมกันและ Deduplicate ด้วย `movie_id` | **Trade-off:** เพิ่มจำนวน API Requests เล็กน้อย แต่การันตี Data Completeness 100% |
| **Ingestion Bottleneck** | ข้อมูล Daily Changes มีหลายหลักหลายพัน หากใช้ Worker ตัวเดียวดึงแบบ Sequential จะใช้เวลานานกว่า 30 นาที | **Producer-Consumer Pattern:** ใช้ AWS Lambda รวบรวม Movie IDs ส่งเข้า Amazon SQS เพื่อให้ Lambda Worker Pool ดึงข้อมูลแบบขนาน (Parallel) | **Trade-off:** มี Throughput เพิ่มขึ้นชั่วขณะ แต่ตัดเวลา Ingestion ให้เสร็จสิ้นภายในไม่กี่นาที |
| **Barrier Synchronization** | Lambda Workers รันแบบ Async กระจายตัวอิสระ ทำให้ Airflow ไม่ทราบเวลาที่การประมวลผลเสร็จสิ้นแน่นอน | **7-Minute Synchronization Delay:** กำหนด Static Delay 7 นาทีใน Airflow (อ้างอิงจากเวลาทำงานปกติไม่เกิน 5 นาที + เผื่อ Safety Buffer 2 นาที) | **Trade-off:** ต้องหน่วงเวลารอ 2 นาที แต่ตัดความเสี่ยง Partial Data Loading ก่อนเริ่ม dbt |
| **Server Memory Spikes** | Docker Compose บน EC2 รัน Airflow เจอ OOM (Out-of-Memory) จน OS สั่งปิดโปรเซส | **OS Swap Memory Tuning:** จัดสรร 4GB Swap Space บน Ubuntu Host เพื่อรับโหลดพีกชั่วขณะ | **Trade-off:** ดิสก์ I/O ช้ากว่า RAM แต่ระบบเสถียร 100% โดยไม่ต้องจ่ายค่าเครื่องแพงขึ้น |

---

## 4. ตาราง ELT Optimization Matrix

| Optimization Technique | Layer | Implementation Details | Quantitative / Technical Impact |
|---|---|---|---|
| **Hive-style Partitioning** | Storage (S3) | จัดโครงสร้าง Path เป็น `year=.../month=.../day=...` ใน Glue Catalog | **ลด Data Scanned > 90%** ใน Athena เพราะอ่านเฉพาะโฟลเดอร์ของวันนั้น ข้ามข้อมูลเก่าทั้งหมด |
| **Strict NDJSON Ingestion** | Ingestion | ข้อมูลดิบใน `raw/` เก็บเป็น Newline Delimited JSON (NDJSON) เท่านั้น | รองรับ **Line-by-Line Streaming** ขจัด Memory Overhead บน Athena Worker จากการโหลด Root JSON Array |
| **Parallel Processing via SQS** | Ingestion | ใช้ SQS คั่นกลางระหว่าง Producer และ Worker เพื่อทำ Parallel Scaling | ลดระยะเวลา Ingestion จากเดิม **30+ นาที เหลือไม่กี่นาที** รองรับปริมาณงานที่เพิ่มขึ้นได้ยืดหยุ่น |
| **Pre-unnested Bridges** | Data Modeling | แตก Nested Arrays (Cast, Crew, Companies, Genres) เป็น Bridge Tables ล่วงหน้า | ขจัดภาระ `CROSS JOIN UNNEST()` สดๆ ทำให้ Dashboard Query ตอบสนองในระดับ Milliseconds |
| **Incremental Merge** | Transformation (dbt) | ใช้กลยุทธ์ `incremental` ร่วมกับ `merge` ในโมเดลส่วนใหญ่ | ประมวลผลและเขียนเฉพาะ Delta Records ลดทั้งเวลาประมวลผลและค่าเขียนไฟล์บน S3 |
| **Analytical Pre-computation** | Analytics | คำนวณ Smooth TF-IDF (8 ระดับมิติ) และ Materialize ลงตาราง Fact ล่วงหน้า | ลดภาระการคำนวณ Text Mining จากหลายนาทีบน BI ให้เรียกใช้งานได้ทันที |
| **OS Memory Swap** | Orchestration | จัดสรร 4GB Swap Space บนโฮสต์ EC2 | ป้องกัน Linux OOM Killer ปิดการทำงานของ Airflow Scheduler/Worker ระหว่าง Memory Spikes |

---

## 5. แบบจำลองข้อมูลระดับ Marts (Dimensional Data Model)

สถาปัตยกรรมข้อมูลถูกออกแบบตามหลักการ **Dimensional Modeling (Star Schema)** ผ่าน `dbt-athena`:

| Table Name | Layer Type | Grain (ระดับความละเอียดของข้อมูล) | Key Columns & Highlight Features |
|---|---|---|---|
| `dim_movies` | Dimension | 1 แถว ต่อ 1 ภาพยนตร์ | Surrogate Key (`movie_sur_id`), ตัวเลขการเงิน (Profit, ROI, Revenue Multiplier) |
| `dim_persons` | Dimension | 1 แถว ต่อ 1 บุคคล (นักแสดง/ผู้กำกับ) | Surrogate Key (`person_sur_id`), กรองข้อมูลโปรไฟล์รูปภาพที่ดีที่สุด |
| `dim_companies` | Dimension | 1 แถว ต่อ 1 ค่าย/สตูดิโอผู้ผลิต | Surrogate Key (`company_sur_id`), โลโก้สตูดิโอ |
| `dim_genres` | Dimension | 1 แถว ต่อ 1 แนวภาพยนตร์ | รองรับ Fallback Default: `-1: None`, `-2: Invalid` |
| `dim_keywords` | Dimension | 1 แถว ต่อ 1 คำสำคัญ (Keyword) | `merge_update_columns = []` เพื่อการทำงานแบบ Insert-only |
| `dim_dates` | Dimension | 1 แถว ต่อ 1 วันในปฏิทิน | Programmatic Generation (2016–2030) ไม่ต้องพึ่งพาไฟล์ CSV ภายนอก |
| `dim_countries` | Dimension | 1 แถว ต่อ 1 ประเทศ | ISO-3166-1 Key พร้อมค่า Default `-1` และ `-2` |
| `bridge_movie_*` | Bridge (M:N) | 1 แถว ต่อ 1 คู่ความสัมพันธ์ | Composite Surrogate Keys, ตัดความซ้ำซ้อนด้วย `UNION`, Insert-only Merge |
| `fact_movie_daily_engagement_100d` | Fact (Periodic) | 1 ภาพยนตร์ ต่อ 1 วันที่วัดผล | **Partitioned by `days_since_release`** รองรับ Retention Curve Analysis |
| `fact_movie_keyword_tfidf` | Fact (Analytical) | 1 คำค้นหา ต่อ 1 Slice การวิเคราะห์ | **Partitioned by `aggregation_level`**, Materialize ค่า Smooth TF-IDF ครบทั้ง 8 ระดับ |

---

## 6. การนำส่งโค้ดอัตโนมัติ (CI/CD Workflows)

ทำงานผ่าน **GitHub Actions** แยกตาม Path Filters:
* `deploy-lambda-snapshot.yml`: สร้าง Docker Image ขึ้น ECR และอัปเดตฟังก์ชัน Snapshot Lambda
* `deploy-lambda-producer.yml`: สร้าง Docker Image ขึ้น ECR และอัปเดตฟังก์ชัน Producer Lambda
* `deploy-lambda-worker.yml`: สร้าง Docker Image ขึ้น ECR และอัปเดตฟังก์ชัน Worker Lambda
* `deploy-airflow-server.yml`: เชื่อมต่อผ่าน SSH ไปยัง EC2 เพื่อสั่ง `git pull` และ `docker compose up -d --build` เมื่อโค้ดฝั่ง Airflow หรือ dbt มีการแก้ไข

---

## 7. โครงสร้างโปรเจกต์ (Repository Structure)

```plaintext
├── .github/workflows/
│   ├── deploy-airflow-server.yml
│   ├── deploy-lambda-producer.yml
│   ├── deploy-lambda-snapshot.yml
│   └── deploy-lambda-worker.yml
├── airflow/
│   ├── dags/
│   │   └── movie_data_pipeline_orchestration.py
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   └── .env.example
├── data_transformation_dbt/
│   ├── models/
│   │   ├── staging/        # ทำความสะอาดข้อมูลและ Cast Data Types
│   │   ├── intermediate/   # คำนวณตัวเลขการเงิน (Profit, ROI)
│   │   └── marts/
│   │       ├── dimensions/ # ตารางมิติพร้อม Surrogate Keys
│   │       ├── bridges/    # ตารางสะพานเชื่อมความสัมพันธ์ M:N
│   │       └── facts/      # ตาราง Fact วัดกระแสและคะแนน TF-IDF
│   ├── dbt_project.yml
│   └── profiles.yml
├── fetcher_for_daily_snapshot/
│   ├── Dockerfile
│   ├── lambda_snapshot.py
│   └── requirements.txt
├── producer_for_daily_changes/
│   ├── Dockerfile
│   ├── lambda_producer.py
│   └── requirements.txt
├── worker_for_daily_changes/
│   ├── Dockerfile
│   ├── lambda_worker.py
│   └── requirements.txt
├── scripts/
│   └── backfill_historical_data.py
├── pyproject.toml
└── README.md
```

---

## 8. วิธีการติดตั้งและรันระบบ (Setup & Configuration)

### 8.1 สิ่งที่ต้องเตรียมบน AWS (Cloud Prerequisites)
* **Amazon S3:** Bucket สำหรับจัดเก็บข้อมูลบน Data Lakehouse
  * โฟลเดอร์ `raw/`: เก็บข้อมูลดิบในรูปแบบ **NDJSON (Newline Delimited JSON) เท่านั้น**
  * โฟลเดอร์ `gold/`: เก็บตารางปลายทางระดับ Marts (Parquet Format)
* **Amazon ECR:** 3 Repositories (`fetcher_for_daily_snapshot`, `producer_for_daily_changes`, `worker_for_daily_changes`)
* **AWS Lambda:** 3 ฟังก์ชันที่ผูกกับ ECR Images
* **Amazon SQS:** 1 Standard Queue สำหรับคั่นคิวงาน Movie IDs
* **AWS Glue & Athena:** 
  * สร้าง **Database Schema บน Glue Data Catalog** ตามฟิลด์และ Data Types เดียวกับข้อมูลดิบในโฟลเดอร์ `raw/` แบบ 1:1
  * สร้าง **Workgroup บน Athena** สำหรับรองรับการประมวลผล Transformation ของ dbt-athena
* **AWS EC2:** Ubuntu Server ติดตั้ง Docker, Docker Compose และเปิด 4GB Swap Space

### 8.2 การกำหนด Environment Variables บน AWS Lambda
* **Snapshot Lambda:**
  * `TMDB_API_KEY`: API Key สำหรับดึงข้อมูลจาก TMDB
  * `S3_BUCKET_NAME`: ชื่อ S3 Bucket ปลายทาง
* **Producer Lambda:**
  * `TMDB_API_KEY`: API Key สำหรับดึงข้อมูลจาก TMDB
  * `SQS_QUEUE_URL`: URL ของคิว Amazon SQS
* **Worker Lambda:**
  * `TMDB_API_KEY`: API Key สำหรับดึงข้อมูลจาก TMDB
  * `S3_BUCKET_NAME`: ชื่อ S3 Bucket ปลายทาง

### 8.3 การกำหนด Security & CI/CD Credentials
1. **GitHub Secrets:** กำหนดตัวแปรใน Repository Settings -> Secrets and variables -> Actions:
   * `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ACCOUNT_ID`
   * `EC2_AIRFLOW_SERVER_HOST`: Public IP ของ EC2
   * `EC2_AIRFLOW_SERVER_USER`: User ของ EC2 (เช่น `ubuntu`)
   * `EC2_AIRFLOW_SERVER_SSH_KEY`: Private Key สำหรับเชื่อมต่อ SSH ไปยัง EC2
2. **GitHub Deploy Keys:**
   * นำ Public Key ของเครื่อง EC2 ไปเพิ่มใน GitHub Repository -> Settings -> Deploy keys เพื่ออนุญาตให้เซิร์ฟเวอร์สามารถสั่ง `git pull` โค้ดลงมาได้
3. **Airflow Environment Configuration:**
   * คัดลอกและตั้งค่าในโฟลเดอร์ `airflow/`:
     ```bash
     cd airflow
     cp .env.example .env
     # กำหนด AIRFLOW_UID=50000 และตั้งค่าการเชื่อมต่อ AWS / Athena
     ```

### 8.4 การรันระบบ (Execution)

#### 8.4.1 การเตรียมข้อมูลย้อนหลัง (Initial Historical Backfill - Optional)

หากต้องการโหลดข้อมูลภาพยนตร์ย้อนหลัง 10 ปี (2016–ปัจจุบัน) เพื่อสร้างชุดข้อมูลเริ่มต้นสำหรับการวิเคราะห์ภาพรวมในระยะยาว สามารถรันสคริปต์เพื่อดึงข้อมูลจาก TMDB API และบันทึกลง Amazon S3 ได้โดยตรง

\```bash
# ติดตั้ง Dependencies และรันสคริปต์ผ่าน uv
uv run python scripts/backfill_historical_data.py
\```

> **หมายเหตุ:** ขั้นตอนนี้เป็นทางเลือก สามารถข้ามได้หากไม่ต้องการโหลดข้อมูลย้อนหลัง

### 8.4.2 เริ่มต้นระบบ Orchestration บน EC2

เข้าไปยังโฟลเดอร์ `airflow` และรัน Docker Compose เพื่อเริ่มต้นบริการ Apache Airflow

```bash
cd airflow

# Build และเริ่มต้น Containers ใน Background
docker compose up -d --build
```

เมื่อระบบเริ่มทำงานแล้ว สามารถเข้าใช้งาน Airflow Web UI ผ่าน Browser ได้ที่:

- **URL:** `http://<EC2_IP>:8088`
- **DAG ID:** `movie_data_pipeline_orchestration`

### 8.4.3 การรัน dbt Models แบบ Manual (Optional)

หากต้องการสั่งรันกระบวนการ Data Transformation ผ่าน dbt Core ด้วยตนเอง สามารถใช้คำสั่งต่อไปนี้จากโฟลเดอร์ `data_transformation_dbt`

```bash
cd data_transformation_dbt

# ติดตั้ง dbt Packages
dbt deps

# รัน Data Transformation Models
dbt run

# ตรวจสอบคุณภาพข้อมูลด้วย Tests
dbt test
```

---


## EN-Version

---

## Overview

An event-driven ELT Data Lakehouse built on a combination of serverless
and container-based AWS services. The pipeline ingests movie data from
the TMDB API, stores raw data in Amazon S3, and transforms it into a
dimensional model (Star Schema) using dbt Core and Amazon Athena. Apache
Airflow, hosted on AWS EC2, orchestrates the workflow, while GitHub
Actions automates Continuous Deployment (CD).

---

## 1. Motivation & Objectives

This project implements an automated Data Lakehouse using an
event-driven architecture and an ELT pipeline to ingest, transform, and
model movie data from the TMDB API. The resulting data supports analysis
across three key areas:

-   **Financial ROI Analysis:** Evaluating the financial performance and
    return on investment of movies.
-   **30-Day Theatrical Trajectory:** Tracking changes in movie
    popularity during the first 30 days after release.
-   **TF-IDF Keyword Analysis:** Identifying prominent content-related
    themes through keyword-level TF-IDF calculations.

The system follows a **Decoupled Storage & Compute** approach on AWS.
Amazon S3 provides data storage, AWS Glue Data Catalog manages metadata,
Amazon Athena handles data querying, and dbt Core performs
transformations and data modeling. Apache Airflow orchestrates the
pipeline, while GitHub Actions automates Continuous Deployment (CD).

### Business Problem

Investment in the film industry involves substantial risk. Decisions
about financing or developing film projects should therefore be
supported by more than intuition or a movie's final gross revenue alone.
Several areas can be explored through data to support better-informed
decisions:

-   **Popularity Trends:** There is a need for a way to monitor and
    compare movie popularity during the first 30 days after release,
    providing insight into audience interest during a critical period of
    theatrical performance.
-   **Movie Content Themes:** There is limited visibility into which
    themes and keywords are prominent or frequently used across
    different years, genres, and countries. Analyzing these patterns may
    help producers identify content trends and explore opportunities to
    develop stories that stand out in the market.

### Engineering Objectives

Design and implement a stable, scalable Lakehouse Architecture with
appropriate control over cloud processing costs. The system uses a
**Decoupled Storage & Compute** architecture to separate data storage
from data processing, alongside an automated ELT pipeline orchestrated
by Apache Airflow and GitHub Actions.

The goal is to produce **analytics-ready data** for BI dashboards that
support financial performance analysis, movie popularity tracking, and
the discovery of prominent content themes that may inspire the
development of future film projects.

---

## 2. System Architecture Flow

``` text
[Airflow DAG: movie_data_pipeline_orchestration]
 │
 ├─ 1. invoke_snapshot_lambda ──> [AWS Lambda: Snapshot]
 │                                       │ (TMDB Discover API: split into 0–14 and 15–30 day windows)
 │                                       ▼
 │                                [S3: raw/movie_daily_snapshot/...] (NDJSON only)
 │
 ├─ 2. invoke_producer_lambda ──> [AWS Lambda: Producer]
 │                                       │ (TMDB Changes API: /movie/changes)
 │                                       ▼
 │                                [Amazon SQS Queue (Task Distribution Buffer)]
 │                                       │
 │                                       ▼ (Concurrency Scaling)
 │                                [AWS Lambda: Worker Pool]
 │                                       │ (TMDB Detail API: /movie/{id})
 │                                       ▼
 │                                [S3: raw/movie_daily_changes/...] (NDJSON only)
 │
 ├─ 3. wait_for_workers_completion (7-minute delay to allow Workers to finish processing)
 │
 └─ 4. trigger_dbt_daily_models ──> [dbt-athena on Airflow/EC2]
                                          │
                                          ▼ (Dimensional Modeling: Staging -> Intermediate -> Marts)
                                   [AWS Athena + AWS Glue Catalog]
                                          │
                                          ▼
                                   [S3 Target Storage: gold/ (Parquet / Marts)]
```

---

## 3. Technical Challenges & Engineering Trade-offs

| Engineering Challenge | Bottleneck / Failure Mode | Architectural Solution | Trade-off & Mitigation |
|---|---|---|---|
| **500-Page API Limit** | The `/discover/movie` endpoint has a maximum retrieval limit of 500 pages, creating a risk of data loss during periods with a high volume of movie releases. | **Window Splitting Strategy:** Divide the search period into two windows: `0–14 days` and `15–30 days`. Combine the results and deduplicate them using `movie_id`. | **Trade-off:** Slightly increases the number of API requests while aiming to improve data completeness. |
| **Ingestion Bottleneck** | Daily Changes may contain several thousand movie IDs. Fetching details sequentially with a single worker can take more than 30 minutes. | **Producer-Consumer Pattern:** An AWS Lambda Producer collects movie IDs and sends them to Amazon SQS. A Lambda Worker Pool then retrieves movie details in parallel. | **Trade-off:** Introduces temporary throughput spikes, while reducing ingestion time to a few minutes. |
| **Barrier Synchronization** | Lambda Workers execute asynchronously and independently, so Airflow does not inherently know when all processing has completed. | **7-Minute Synchronization Delay:** Configure a static 7-minute delay in Airflow, based on a typical processing time of no more than 5 minutes plus a 2-minute safety buffer. | **Trade-off:** Adds waiting time, but reduces the risk of dbt starting before ingestion has finished. |
| **Server Memory Spikes** | Airflow running through Docker Compose on EC2 experienced out-of-memory (OOM) events, causing the operating system to terminate processes. | **OS Swap Memory Tuning:** Allocate 4 GB of swap space on the Ubuntu host to absorb temporary memory spikes. | **Trade-off:** Disk I/O is slower than RAM, but swap provides additional memory headroom without upgrading to a more expensive instance. |

---

## 4. ELT Optimization Matrix

| Optimization Technique | Layer | Implementation Details | Quantitative / Technical Impact |
|---|---|---|---|
| **Hive-Style Partitioning** | Storage (S3) | Organize S3 paths using `year=.../month=.../day=...` and register the partitions in Glue Catalog. | Can reduce Athena data scanned by **more than 90%** for date-filtered queries when only the relevant daily partitions need to be read. |
| **Strict NDJSON Ingestion** | Ingestion | Store raw data under `raw/` exclusively in Newline Delimited JSON (NDJSON) format. | Supports record-by-record JSON processing and avoids the memory overhead associated with parsing a single root JSON array. |
| **Parallel Processing via SQS** | Ingestion | Use SQS as a buffer between the Producer and Workers to enable parallel processing. | Reduces ingestion time from **30+ minutes to a few minutes**, while allowing the workload to scale more flexibly. |
| **Pre-unnested Bridge Tables** | Data Modeling | Extract nested arrays (Cast, Crew, Companies, and Genres) into bridge tables ahead of analytical queries. | Avoids repeatedly executing `CROSS JOIN UNNEST()` at query time and can improve dashboard query performance. |
| **Incremental Merge** | Transformation (dbt) | Use dbt `incremental` materialization with the `merge` strategy for most models. | Processes and writes only delta records, reducing transformation time and the amount of data written to S3. |
| **Analytical Pre-computation** | Analytics | Calculate Smooth TF-IDF across eight analytical dimensions and materialize the results in advance. | Moves text-mining computation out of BI queries, allowing dashboards to retrieve precomputed results directly. |
| **OS Memory Swap** | Orchestration | Allocate 4 GB of swap space on the EC2 host. | Provides additional memory headroom and helps reduce the risk of the Linux OOM Killer terminating Airflow Scheduler or Worker processes during memory spikes. |

---

## 5. Dimensional Data Model (Marts)

The data architecture follows **Dimensional Modeling (Star Schema)** principles and is implemented using `dbt-athena`.

| Table Name | Layer Type | Grain | Key Columns & Highlights |
|---|---|---|---|
| `dim_movies` | Dimension | One row per movie | Surrogate Key (`movie_sur_id`), financial metrics (Profit, ROI, Revenue Multiplier). |
| `dim_persons` | Dimension | One row per person (cast or crew) | Surrogate Key (`person_sur_id`), with logic to select the best available profile image. |
| `dim_companies` | Dimension | One row per production company or studio | Surrogate Key (`company_sur_id`), studio logo. |
| `dim_genres` | Dimension | One row per movie genre | Supports default fallback records: `-1: None`, `-2: Invalid`. |
| `dim_keywords` | Dimension | One row per keyword | Uses `merge_update_columns = []` to implement insert-only behavior. |
| `dim_dates` | Dimension | One row per calendar date | Programmatically generated for 2016–2030 without relying on an external CSV file. |
| `dim_countries` | Dimension | One row per country | Uses ISO-3166-1 keys with default records `-1` and `-2`. |
| `bridge_movie_*` | Bridge (M:N) | One row per relationship pair | Composite surrogate keys, duplicate removal using `UNION`, and insert-only merge behavior. |
| `fact_movie_daily_engagement_100d` | Fact (Periodic Snapshot) | One movie per measurement date | **Partitioned by `days_since_release`** to support retention-curve analysis. |
| `fact_movie_keyword_tfidf` | Fact (Analytical) | One keyword per analytical slice | **Partitioned by `aggregation_level`**; stores precomputed Smooth TF-IDF values across all eight levels. |

---

## 6. Automated Deployment (CI/CD Workflows)

GitHub Actions workflows are separated using path filters:

-   `deploy-lambda-snapshot.yml`: Builds a Docker image, pushes it to
    Amazon ECR, and updates the Snapshot Lambda function.
-   `deploy-lambda-producer.yml`: Builds a Docker image, pushes it to
    Amazon ECR, and updates the Producer Lambda function.
-   `deploy-lambda-worker.yml`: Builds a Docker image, pushes it to
    Amazon ECR, and updates the Worker Lambda function.
-   `deploy-airflow-server.yml`: Connects to EC2 over SSH and runs
    `git pull` followed by `docker compose up -d --build` when Airflow
    or dbt code changes.

---

## 7. Repository Structure

``` text
├── .github/workflows/
│   ├── deploy-airflow-server.yml
│   ├── deploy-lambda-producer.yml
│   ├── deploy-lambda-snapshot.yml
│   └── deploy-lambda-worker.yml
├── airflow/
│   ├── dags/
│   │   └── movie_data_pipeline_orchestration.py
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   └── .env.example
├── data_transformation_dbt/
│   ├── models/
│   │   ├── staging/        # Data cleaning and data type casting
│   │   ├── intermediate/   # Financial metric calculations (Profit, ROI)
│   │   └── marts/
│   │       ├── dimensions/ # Dimension tables with surrogate keys
│   │       ├── bridges/    # Bridge tables for M:N relationships
│   │       └── facts/      # Fact tables for engagement and TF-IDF metrics
│   ├── dbt_project.yml
│   └── profiles.yml
├── fetcher_for_daily_snapshot/
│   ├── Dockerfile
│   ├── lambda_snapshot.py
│   └── requirements.txt
├── producer_for_daily_changes/
│   ├── Dockerfile
│   ├── lambda_producer.py
│   └── requirements.txt
├── worker_for_daily_changes/
│   ├── Dockerfile
│   ├── lambda_worker.py
│   └── requirements.txt
├── scripts/
│   └── backfill_historical_data.py
├── pyproject.toml
└── README.md
```

---

## 8. Setup & Configuration

### 8.1 AWS Cloud Prerequisites

-   **Amazon S3:** Create a bucket for the Data Lakehouse.
    -   `raw/`: Stores raw data exclusively in **NDJSON (Newline
        Delimited JSON)** format.
    -   `gold/`: Stores final Marts tables in **Parquet** format.
-   **Amazon ECR:** Create three repositories:
    `fetcher_for_daily_snapshot`, `producer_for_daily_changes`, and
    `worker_for_daily_changes`.
-   **AWS Lambda:** Create three functions and configure each to use its
    corresponding ECR image.
-   **Amazon SQS:** Create one Standard Queue to buffer movie ID tasks.
-   **AWS Glue & Amazon Athena:**
    -   Create a **database schema in Glue Data Catalog** whose fields
        and data types match the raw data in `raw/` on a 1:1 basis.
    -   Create an **Athena Workgroup** for dbt-athena transformations.
-   **AWS EC2:** Provision an Ubuntu Server with Docker and Docker
    Compose installed, and configure 4 GB of swap space.

### 8.2 AWS Lambda Environment Variables

-   **Snapshot Lambda:**
    -   `TMDB_API_KEY`: API key for retrieving data from TMDB.
    -   `S3_BUCKET_NAME`: Destination S3 bucket name.
-   **Producer Lambda:**
    -   `TMDB_API_KEY`: API key for retrieving data from TMDB.
    -   `SQS_QUEUE_URL`: Amazon SQS queue URL.
-   **Worker Lambda:**
    -   `TMDB_API_KEY`: API key for retrieving data from TMDB.
    -   `S3_BUCKET_NAME`: Destination S3 bucket name.

### 8.3 Security & CI/CD Credentials

1.  **GitHub Secrets:** Add the following repository secrets under
    **Settings → Secrets and variables → Actions**:
    -   `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`,
        `AWS_ACCOUNT_ID`
    -   `EC2_AIRFLOW_SERVER_HOST`: Public IP address of the EC2
        instance.
    -   `EC2_AIRFLOW_SERVER_USER`: EC2 login user (for example,
        `ubuntu`).
    -   `EC2_AIRFLOW_SERVER_SSH_KEY`: Private key used to connect to EC2
        over SSH.
2.  **GitHub Deploy Keys:**
    -   Add the EC2 machine's public key under **Repository → Settings →
        Deploy keys** to allow the server to pull the repository using
        `git pull`.
3.  **Airflow Environment Configuration:**
    -   Copy the example environment file and configure it in the
        `airflow/` directory:

    ``` bash
    cd airflow
    cp .env.example .env
    # Set AIRFLOW_UID=50000 and configure AWS / Athena connections
    ```

### 8.4 Running the System

#### 8.4.1 Initial Historical Backfill (Optional)

To load approximately 10 years of historical movie data (2016--present)
and create an initial dataset for long-term analysis, run the script to
retrieve data from the TMDB API and write it directly to Amazon S3.

``` bash
# Install dependencies and run the script with uv
uv run python scripts/backfill_historical_data.py
```

> **Note:** This step is optional and can be skipped if historical data
> is not required.

#### 8.4.2 Start the Orchestration Service on EC2

Navigate to the `airflow` directory and use Docker Compose to start the
Apache Airflow services.

``` bash
cd airflow

# Build the images and start containers in the background
docker compose up -d --build
```

Once the services are running, access the Airflow Web UI in a browser:

-   **URL:** `http://<EC2_IP>:8088`
-   **DAG ID:** `movie_data_pipeline_orchestration`

#### 8.4.3 Run dbt Models Manually (Optional)

To run the data transformation process manually with dbt Core, execute
the following commands from the `data_transformation_dbt` directory:

``` bash
cd data_transformation_dbt

# Install dbt packages
dbt deps

# Run data transformation models
dbt run

# Run data quality tests
dbt test
```

> **Note:** This step is optional. Under normal operation, Apache
> Airflow invokes dbt according to the pipeline execution sequence.

---