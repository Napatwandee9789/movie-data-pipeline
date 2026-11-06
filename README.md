# End-to-End Movie Data Lakehouse & Automated Orchestration | Souce: TMDB

![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Lambda%20%7C%20SQS%20%7C%20Athena%20%7C%20Glue%20%7C%20EC2-232F3E?logo=amazon-aws)
![Apache Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE?logo=apache-airflow)
![dbt](https://img.shields.io/badge/Transformation-dbt--core-FF694B?logo=dbt)
![Docker](https://img.shields.io/badge/Container-Docker-2496ED?logo=docker)
![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-2088FF?logo=github-actions)

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

## 4. ตารางการปรับแต่งความเร็วและต้นทุน (ELT Optimization Matrix)

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

### 1. สิ่งที่ต้องเตรียมบน AWS (Cloud Prerequisites)
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

### 2. การกำหนด Environment Variables บน AWS Lambda
* **Snapshot Lambda:**
  * `TMDB_API_KEY`: API Key สำหรับดึงข้อมูลจาก TMDB
  * `S3_BUCKET_NAME`: ชื่อ S3 Bucket ปลายทาง
* **Producer Lambda:**
  * `TMDB_API_KEY`: API Key สำหรับดึงข้อมูลจาก TMDB
  * `SQS_QUEUE_URL`: URL ของคิว Amazon SQS
* **Worker Lambda:**
  * `TMDB_API_KEY`: API Key สำหรับดึงข้อมูลจาก TMDB
  * `S3_BUCKET_NAME`: ชื่อ S3 Bucket ปลายทาง

### 3. การกำหนด Security & CI/CD Credentials
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

## 4. การรันระบบ (Execution)

### 4.1 การเตรียมข้อมูลย้อนหลัง (Initial Historical Backfill - Optional)

หากต้องการโหลดข้อมูลภาพยนตร์ย้อนหลัง 10 ปี (2016–ปัจจุบัน) เพื่อสร้างชุดข้อมูลเริ่มต้นสำหรับการวิเคราะห์ภาพรวมในระยะยาว สามารถรันสคริปต์เพื่อดึงข้อมูลจาก TMDB API และบันทึกลง Amazon S3 ได้โดยตรง

\```bash
# ติดตั้ง Dependencies และรันสคริปต์ผ่าน uv
uv run python scripts/backfill_historical_data.py
\```

> **หมายเหตุ:** ขั้นตอนนี้เป็นทางเลือก สามารถข้ามได้หากไม่ต้องการโหลดข้อมูลย้อนหลัง

### 4.2 เริ่มต้นระบบ Orchestration บน EC2

เข้าไปยังโฟลเดอร์ `airflow` และรัน Docker Compose เพื่อเริ่มต้นบริการ Apache Airflow

\```bash
cd airflow

# Build และเริ่มต้น Containers ใน Background
docker compose up -d --build
\```

เมื่อระบบเริ่มทำงานแล้ว สามารถเข้าใช้งาน Airflow Web UI ผ่าน Browser ได้ที่:

- **URL:** `http://<EC2_IP>:8088`
- **DAG ID:** `movie_data_pipeline_orchestration`

### 4.3 การรัน dbt Models แบบ Manual (Optional)

หากต้องการสั่งรันกระบวนการ Data Transformation ผ่าน dbt Core ด้วยตนเอง สามารถใช้คำสั่งต่อไปนี้จากโฟลเดอร์ `data_transformation_dbt`

\```bash
cd data_transformation_dbt

# ติดตั้ง dbt Packages
dbt deps

# รัน Data Transformation Models
dbt run

# ตรวจสอบคุณภาพข้อมูลด้วย Tests
dbt test
\```

> **หมายเหตุ:** ขั้นตอนนี้เป็นทางเลือก โดยปกติ dbt จะถูกเรียกใช้งานผ่าน Apache Airflow ตามลำดับของ Pipeline ที่กำหนดไว้