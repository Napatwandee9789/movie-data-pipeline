# End-to-End Movie Data Lakehouse & Automated Orchestration | Souce: TMDB

![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Lambda%20%7C%20SQS%20%7C%20Athena%20%7C%20Glue%20%7C%20EC2-232F3E?logo=amazon-aws)
![Apache Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE?logo=apache-airflow)
![dbt](https://img.shields.io/badge/Transformation-dbt--core-FF694B?logo=dbt)
![Docker](https://img.shields.io/badge/Container-Docker-2496ED?logo=docker)
![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-2088FF?logo=github-actions)

## Table of Contents

- [Thai Version](#th-version)
  - [1. ที่มาและเป้าหมาย](#1-ที่มาและเป้าหมาย-motivation--objective)
  - [2. แผนภาพสถาปัตยกรรมระบบ](#2-แผนภาพสถาปัตยกรรมระบบ-architecture-flow)
  - [3. ปัญหาทางเทคนิคและการตัดสินใจเชิงวิศวกรรม](#3-ปัญหาทางเทคนิคและการตัดสินใจเชิงวิศวกรรม-technical-challenges--trade-offs)
  - [4. ตาราง ELT Optimization Matrix](#4-ตาราง-elt-optimization-matrix)
  - [5. แบบจำลองข้อมูลระดับ Marts](#5-แบบจำลองข้อมูลระดับ-marts-dimensional-data-model)
  - [6. การนำส่งโค้ดอัตโนมัติ](#6-การนำส่งโค้ดอัตโนมัติ-cicd-workflows)
  - [7. โครงสร้างโปรเจกต์](#7-โครงสร้างโปรเจกต์-repository-structure)
  - [8. วิธีการติดตั้งและรันระบบ](#8-วิธีการติดตั้งและรันระบบ-setup--configuration)

- [English Version](#en-version)
  - [1. Motivation & Objective](#1-motivation--objective)
  - [2. Architecture Flow](#2-architecture-flow)
  - [3. Technical Challenges & Engineering Trade-offs](#3-technical-challenges--engineering-trade-offs)
  - [4. ELT Optimization Matrix](#4-elt-optimization-matrix)
  - [5. Dimensional Data Model (Marts Layer)](#5-dimensional-data-model-marts-layer)
  - [6. Automated CI/CD Workflows](#6-automated-cicd-workflows)
  - [7. Repository Structure](#7-repository-structure)
  - [8. Setup & Configuration](#8-setup--configuration)

## TH-Version

ระบบ Data Lakehouse แบบ Event-Driven ELT Pipeline บนสถาปัตยกรรม Serverless และ Container ดึงข้อมูลภาพยนตร์จาก TMDB API จัดเก็บลง AWS S3 แปลงข้อมูลเป็น Dimensional Model (Star Schema) บน AWS Athena ด้วย dbt Core ควบคุมการทำงานอัตโนมัติด้วย Apache Airflow บน AWS EC2 พร้อมระบบ Continuous Deployment (CD) ผ่าน GitHub Actions

## 1. ที่มาและเป้าหมาย (Motivation & Objective)

ระบบ Automated Data Lakehouse ที่พัฒนาขึ้นโดยใช้สถาปัตยกรรมแบบ Event-Driven และ ELT Pipeline สำหรับดึงข้อมูล แปลงข้อมูล และสร้างแบบจำลองข้อมูลภาพยนตร์จาก TMDB API เพื่อรองรับการวิเคราะห์ข้อมูลใน 3 มิติหลัก ได้แก่

1. **การวิเคราะห์ผลประกอบการทางการเงินและรายได้ Box Office (Financial & Box Office Performance):** ครอบคลุมความสามารถในการทำกำไร การกระจายตัวของรายได้ ช่วงเวลาการเข้าฉายตามฤดูกาล และตลาดภาพยนตร์ระดับโลก
2. **การวิเคราะห์เชิงลึกด้านแก่นเรื่องและประเด็นเนื้อหา (Narrative & Thematic Insights):** เพื่อติดตามวิวัฒนาการความถี่ของ Keyword กระแสหลักและค้นหาเอกลักษณ์เฉพาะตัวของแต่ละประเภทภาพยนตร์ผ่านการคำนวณ TF-IDF
3. **การวิเคราะห์การมีส่วนร่วมและพฤติกรรมผู้ชมในช่วง 30 วันแรกหลังเข้าฉาย (30-Day Movie Engagement & Audience Behavior Analytics):** เพื่อติดตามแรงส่งความนิยม การเติบโตของยอดโหวตสะสม การทรงตัวของคะแนนรีวิว และการลดลงของกระแสความสนใจ (Hype Decay)

ระบบออกแบบตามแนวคิด **Decoupled Storage & Compute** บน AWS โดยใช้ Amazon S3 เป็นพื้นที่จัดเก็บข้อมูล, AWS Glue Data Catalog สำหรับจัดการ Metadata, Amazon Athena สำหรับการ Query ข้อมูล และ dbt Core สำหรับการแปลงและสร้างแบบจำลองข้อมูล พร้อมจัดการลำดับการทำงานของ Pipeline ด้วย Apache Airflow และใช้ GitHub Actions สำหรับทำ Continuous Deployment (CD) โดยอัตโนมัติ

* **ปัญหาทางธุรกิจ:** การลงทุนในอุตสาหกรรมภาพยนตร์มีความเสี่ยงสูง การตัดสินใจลงทุนหรือพัฒนาโครงการภาพยนตร์จึงจำเป็นต้องอาศัยข้อมูลประกอบมากกว่าการพิจารณาจากสัญชาตญาณหรือรายได้รวมหลังภาพยนตร์ออกฉายจบเพียงอย่างเดียว โดยยังมีประเด็นสำคัญที่สามารถนำข้อมูลมาวิเคราะห์เพื่อสนับสนุนการตัดสินใจได้ ได้แก่
  * **แนวโน้มความนิยม:** ขาดเครื่องมือที่ช่วยติดตามและเปรียบเทียบแนวโน้มความนิยมของภาพยนตร์ในช่วง 30 วันแรกหลังเข้าฉาย เพื่อให้เห็นทิศทางความนิยมในช่วงเวลาสำคัญของการฉาย
  * **ประเด็นเนื้อหาภาพยนตร์:** ขาดมุมมองในการวิเคราะห์ว่าในแต่ละปี ประเภทภาพยนตร์ หรือประเทศ มีประเด็นและ Keyword ใดที่โดดเด่นหรือถูกนำมาใช้บ่อย ซึ่งอาจช่วยให้ผู้ผลิตมองเห็นแนวโน้มของเนื้อหาที่ได้รับความนิยม รวมถึงค้นหาแนวทางสร้างสรรค์เนื้อหาที่แตกต่างจากสิ่งที่มีอยู่ในตลาด
* **เป้าหมายเชิงวิศวกรรม:** ออกแบบและพัฒนา Lakehouse Architecture ที่มีความเสถียร รองรับการขยายตัว (Scalable) และสามารถควบคุมต้นทุนการประมวลผลบนคลาวด์ได้อย่างเหมาะสม โดยใช้สถาปัตยกรรมแบบ Decoupled Storage & Compute เพื่อแยกส่วนการจัดเก็บข้อมูลและการประมวลผลออกจากกัน พร้อมพัฒนา ELT Pipeline ที่ทำงานโดยอัตโนมัติผ่าน Apache Airflow และ GitHub Actions เพื่อจัดเตรียมข้อมูลให้อยู่ในรูปแบบพร้อมใช้งานสำหรับการวิเคราะห์ (Analytics-Ready Data) และนำไปแสดงผลผ่าน Web Dashboard เพื่อสนับสนุนการวิเคราะห์ผลตอบแทนทางการเงิน แนวโน้มความนิยมของภาพยนตร์ และการค้นหาประเด็นเนื้อหาที่โดดเด่นซึ่งสามารถนำไปต่อยอดเป็นแนวคิดในการพัฒนาภาพยนตร์เรื่องใหม่ได้

## 2. แผนภาพสถาปัตยกรรมระบบ (Architecture Flow)

Data Pipeline นี้บริหารจัดการและควบคุมลำดับการทำงานทั้งหมดด้วย **Apache Airflow \[B\]** ซึ่งถูก Deploy และทำงานอยู่บน **AWS EC2 \[A\]** ในรูปแบบ Docker Container โดยทำหน้าที่เป็น Workflow Orchestrator หลักในการตั้งเวลาการทำงาน จัดการ Dependencies เชื่อมโยงระบบ Serverless Ingestion และสั่งรัน dbt Core เพื่อประมวลผลข้อมูลลงสู่ Data Lakehouse

### การบริหารจัดการต้นทุน (Cost Optimization)

* **Scheduled Compute Management (AWS EventBridge \[L\]):** ตั้งตารางเวลาสั่งเปิดและปิดเครื่อง **AWS EC2 \[A\]** อัตโนมัติตามรอบการทำงานของ Pipeline เพื่อหลีกเลี่ยงการเปิดเซิร์ฟเวอร์ทิ้งไว้โดยไม่มีการใช้งาน
* **Serverless Execution (AWS Lambda \[C, E, F\]):** เลือกใช้สถาปัตยกรรมแบบ Serverless สำหรับการดึงข้อมูล ทำให้ไม่ต้องเปิดเซิร์ฟเวอร์ทิ้งไว้ตลอดเวลา โดยระบบจะคิดค่าบริการตามจำนวนครั้งที่เรียกใช้และระยะเวลาการประมวลผลจริงเท่านั้น (GB-seconds) จากการรัน Pipeline รายวันต่อเนื่องเกือบ 1 เดือน พบว่ามีค่าใช้จ่ายในส่วนของ Lambda อยู่ที่ **\$0** เนื่องจากการใช้งานยังอยู่ในโควตา **AWS Always Free Tier** (ฟรี 1,000,000 Requests และ 400,000 GB-seconds ต่อเดือน; หากใช้งานเกินโควตาจะคิดค่าบริการเริ่มต้นเพียง \$0.20 ต่อ 1 ล้าน Requests และ $0.0000166667 ต่อ GB-second)
* **Conditional Monthly Processing (Task 5 & Task 6):** ใช้ Branching Logic ตรวจสอบวันแรกของเดือน เพื่อข้ามการรันโมเดลระดับเดือนในวันปกติ ช่วยลดปริมาณการสแกนข้อมูลและค่าใช้จ่ายในการ Query ของ **AWS Athena \[I\]**

### ระบบ CI/CD Deployment

* **Continuous Deployment to EC2 (GitHub Actions \[M\]** $\rightarrow$ **AWS EC2 \[A\]):** เมื่อมีการ Push โค้ดใหม่ขึ้น Repository ระบบ **GitHub Actions \[M\]** จะทำการเชื่อมต่อและสั่งให้ **AWS EC2 \[A\]** ดึงโค้ดล่าสุด (DAGs, dbt models) จาก GitHub มาอัปเดตบนเครื่องโดยอัตโนมัติ
* **Serverless Container Pipeline (GitHub Actions \[M\]** $\rightarrow$ **Docker \[N\]** $\rightarrow$ **Amazon ECR \[O\]):** ระบบ **GitHub Actions \[M\]** สั่ง **Docker \[N\]** ทำการ Build โค้ดและ Dependencies ของฟังก์ชัน Ingestion ออกมาเป็น Container Image แล้ว Push ขึ้นไปจัดเก็บบน **Amazon ECR \[O\]** เพื่อให้กลุ่มฟังก์ชัน **AWS Lambda \[C, E, F\]** ดึง Image เวอร์ชันล่าสุดไปใช้งาน

### ลำดับการทำงานของ Data Pipeline (DAG Workflow)

#### Task 1: `invoke_producer_lambda`

* **Apache Airflow \[B\]** บน **AWS EC2 \[A\]** สั่งทริกเกอร์ **AWS Lambda (Producer) \[C\]** เพื่อเริ่มต้น Pipeline การนำเข้าข้อมูล
* **AWS Lambda (Producer) \[C\]** ดึงรายการ Movie IDs ที่มีการอัปเดตรายวัน (Daily Changed Movie IDs) จาก TMDB API
* แบ่งชุด IDs เป็น Batches แล้วส่งเข้าคิว **AWS Simple Queue Service (SQS) \[D\]** เพื่อทำหน้าที่เป็นบัฟเฟอร์และควบคุม Concurrency ไม่ให้เกิน Rate Limit ของ API
* **AWS Lambda (Workers) \[E\]** ถูกเรียกใช้งานจาก **SQS \[D\]** เพื่อดึงข้อมูลรายละเอียดเชิงลึกของภาพยนตร์ (Movie Details เช่น genres, runtime, release date) แบบคู่ขนาน (Scalable Processing) เพื่อไม่ให้ Worker ตัวเดียวทำงานหนักเกินไป
* **AWS Lambda (Workers) \[E\]** บันทึกข้อมูลดิบรูปแบบ Raw JSON ลงใน **Amazon S3 (Raw Zone) \[G\]**

#### Task 2: `wait_for_workers_completion`

* หยุดรอเวลาเป็นเวลา 7 นาที เพื่อเปิดช่วงเวลาให้ **AWS Lambda (Workers) \[E\]** ทยอยดึงงานจากคิวและประมวลผลข้อมูลจนเสร็จสมบูรณ์
* โดยปกติ Workers จะใช้เวลาทำงานไม่เกิน 5 นาที แต่ตั้งหน่วงเวลาเผื่อไว้อีก 2 นาทีเพื่อรองรับวันที่มีจำนวนภาพยนตร์ที่มีการอัปเดตหรือเปลี่ยนแปลงมากกว่าปกติ
* ช่วยให้มั่นใจว่ากระบวนการดึง Movie Details จาก API และการเขียนไฟล์ Raw JSON ทั้งหมดลง **Amazon S3 (Raw Zone) \[G\]** เสร็จสิ้นเรียบร้อย
* ทำหน้าที่เป็นจุดรอข้อมูลให้พร้อมก่อนไปต่อ โดยรวมสายการทำงานเข้ากับ **Task 3** เพื่อการันตีว่าข้อมูลดิบทั้ง Movie Details และ Daily Snapshots ถูกบันทึกครบถ้วน ก่อนเริ่มรัน dbt ในขั้นตอนถัดไป

#### Task 3: `invoke_snapshot_lambda`

* ทำงานแบบคู่ขนาน (Parallel) พร้อมกับ **Task 1**
* **Apache Airflow \[B\]** สั่งทริกเกอร์ **AWS Lambda \[F\]**
* **AWS Lambda \[F\]** ดึงข้อมูลสถิติความนิยมและการมีส่วนร่วมจากผู้ชมของภาพยนตร์ที่เข้าฉายในช่วง 30 วันที่ผ่านมาแบบรายวัน (Daily Engagement Metrics เช่น popularity, vote count, vote average) จาก TMDB API
* บันทึกข้อมูล Raw JSON รูปแบบ NDJSON ลงใน **Amazon S3 (Raw Zone) \[G\]** โดยตรง

#### Task 4: `trigger_dbt_daily_models`

* **Apache Airflow \[B\]** สั่งรัน CLI ของ **dbt Core \[K\]** บนคอนเทนเนอร์
* **dbt Core \[K\]** Compile โค้ด SQL และส่งคำสั่งผ่าน adapter `dbt-athena-community` ไปประมวลผลบน **AWS Athena \[I\]**
* **AWS Athena \[I\]** อ้างอิง Metadata โครงสร้างตารางและ Partition จาก **AWS Glue Data Catalog \[H\]** เพื่อ Query อ่านไฟล์ Raw JSON จาก **Amazon S3 (Raw Zone) \[G\]**
* ประมวลผลสร้างข้อมูลรายวัน ได้แก่ ตารางมิติ (Dimensions), ตารางเชื่อมโยง (Bridges เช่น ประเภทหนัง, นักแสดง) และตารางสถิติความนิยมและการมีส่วนร่วมจากผู้ชมของภาพยนตร์ที่เข้าฉายในช่วง 30 วันที่ผ่านมาแบบรายวัน (`fact_movie_daily_engagement_30d`)
* บันทึกผลลัพธ์เป็นไฟล์ Parquet ลงใน **Amazon S3 (Gold Zone / Lakehouse) \[J\]**

#### Task 5: `evaluate_calendar_boundary`

* ทำหน้าที่เป็น Branching Operator ตรวจสอบเงื่อนไขว่า Execution Date ของรอบนี้ตรงกับ **วันแรกของเดือน (1st of month)** หรือไม่
* **กรณีไม่ใช่ (No):** ส่งต่อไปยัง **Task 6: `skip_monthly_transformation`** เพื่อจบการทำงานของ Pipeline ประจำวันทันที
* **กรณีใช่ (Yes):** ส่งต่อไปยัง **Task 7: `trigger_dbt_monthly_models`** เพื่อประมวลผลรอบพิเศษประจำเดือน

#### Task 6: `skip_monthly_transformation`

* ทำหน้าที่เป็น Dummy Task (ผ่าน `EmptyOperator`) สำหรับรองรับเส้นทางการทำงานกรณีที่ไม่ใช่วันแรกของเดือน
* จบการทำงานของ Pipeline ประจำวันทันที โดยไม่สั่งรันโมเดลระดับเดือน เพื่อหลีกเลี่ยงการใช้ทรัพยากรและลดค่าใช้จ่ายในการคิวรีของ **AWS Athena \[I\]** โดยไม่จำเป็น

#### Task 7: `trigger_dbt_monthly_models`

* **Apache Airflow \[B\]** สั่งรันคำสั่ง **dbt Core \[K\]** สำหรับโมเดลรายเดือน
* **dbt Core \[K\]** ส่งคำสั่ง SQL ไปสั่ง **AWS Athena \[I\]** ให้ประมวลผลโมเดล `fact_movie_keyword_tfidf` ใหม่ทั้งหมดทั้งคลังข้อมูล (Full Refresh ในรูปแบบ `materialized='table'`)
* **AWS Athena \[I\]** คำนวณสรุปข้อมูลภาพยนตร์รายเดือน เช่น การรวมยอดสถิติรายเดือน (Monthly Aggregated Metrics), แนวโน้มความนิยมสะสม (Monthly Popularity Trends) และตัวชี้วัดประสิทธิภาพรายเดือน (Monthly Performance Indicators)
* เขียนไฟล์ผลลัพธ์ Parquet จัดเก็บลงใน **Amazon S3 (Gold Zone / Lakehouse) \[J\]**

## 3. ปัญหาทางเทคนิคและการตัดสินใจเชิงวิศวกรรม (Technical Challenges & Trade-offs)

| Engineering Challenge | The Bottleneck / Failure Mode | Architectural Solution | Trade-off & Mitigation |
| :--- | :--- | :--- | :--- |
| **500-Page API Limit** | ข้อมูล Daily Snapshot มีการใช้ Endpoint `/discover/movie` เพื่อดึงข้อมูล ซึ่งจำกัดการดึงสูงสุด 500 หน้า เสี่ยงเกิด Data Loss ช่วงที่มีภาพยนตร์เข้าฉายพร้อมกันจำนวนมาก | **Window Splitting Strategy:** ซอยช่วงเวลาค้นหาเป็น 2 กรอบ: `0–14 วัน` และ `15–30 วัน` แล้วนำมารวมกันและ Deduplicate ด้วย `movie_id` | **Trade-off:** เพิ่มจำนวน API Requests เล็กน้อย แต่การันตี Data Completeness 100% |
| **Ingestion Bottleneck** | ข้อมูล Daily Changes มีหลักพัน - หลักหมื่นเรื่อง หากใช้ Worker ตัวเดียวดึงแบบ Sequential จะใช้เวลานานกว่า 30 นาที และเสี่ยงชน **Lambda Timeout** จนถูกตัดกลางคัน ทำให้ข้อมูลที่ดึงค้างใน RAM หายและ SQS เด้งซ้ำทั้ง batch | **Producer-Consumer Pattern:** ใช้ AWS Lambda รวบรวม Movie IDs ส่งเข้า Amazon SQS เพื่อให้ Lambda Worker Pool ดึงข้อมูลแบบขนาน (Parallel) | **Trade-off:** มี Throughput เพิ่มขึ้นชั่วขณะทำให้ลดโอกาสชน Timeout แต่ต้องคุม Batch Size ต่อ Message, Rate Limit ของ TMDB รวมถึงออกแบบกระบวนการปลายทางให้รองรับการประมวลผลซ้ำโดยไม่เกิดข้อมูลซ้ำซ้อน (Idempotency) |
| **Barrier Synchronization** | Lambda Workers รันแบบ Async กระจายตัวอิสระ ทำให้ Airflow ไม่ทราบเวลาที่การประมวลผลเสร็จสิ้นแน่นอน | **7-Minute Synchronization Delay:** กำหนด Static Delay 7 นาทีใน Airflow (อ้างอิงจากเวลาทำงานปกติไม่เกิน 5 นาที + เผื่อ Safety Buffer 2 นาที) | **Trade-off:** ต้องหน่วงเวลารอ 2 นาที แต่ตัดความเสี่ยง Partial Data Loading ก่อนเริ่ม dbt |
| **Server Memory Spikes** | Docker Compose บน EC2 รัน Airflow เจอ OOM (Out-of-Memory) จน OS สั่งปิดโปรเซส | **OS Swap Memory Tuning:** จัดสรร 4GB Swap Space บน Ubuntu Host เพื่อรับโหลดพีกชั่วขณะ | **Trade-off:** ดิสก์ I/O ช้ากว่า RAM แต่ระบบเสถียร 100% โดยไม่ต้องจ่ายค่าเครื่องแพงขึ้น |

## 4. ตาราง ELT Optimization Matrix

| Optimization Technique | Layer | Implementation Details | Quantitative / Technical Impact |
| :--- | :--- | :--- | :--- |
| **Hive-style Partitioning** | Raw Storage (S3) | จัดโครงสร้าง Path ของข้อมูลดิบที่นำเข้ารายวันเป็น `year=.../month=.../day=...` และลงทะเบียนใน Glue Catalog | **ลด Data Scanned ได้ \~87.5%** ใน Athena (จาก 98.11 MB เหลือ 12.27 MB จากการทดสอบ Partition Pruning) โดยอ่านเฉพาะโฟลเดอร์ของวันนั้น |
| **Parallel Processing via SQS** | Ingestion | ใช้ SQS คั่นกลางระหว่าง Producer และ Worker เพื่อทำ Parallel Scaling | ลดระยะเวลา Ingestion จากเดิม **30+ นาที เหลือไม่กี่นาที** รองรับปริมาณงานที่เพิ่มขึ้นได้ยืดหยุ่น |
| **Pre-unnested Bridges** | Data Modeling | แตก Nested Arrays (Companies, Genres) เป็น Bridge Tables ล่วงหน้า | ไม่มีภาระจากการ `CROSS JOIN UNNEST()` สด ๆ ทำให้ Dashboard Query ตอบสนองในระดับ Milliseconds |
| **Incremental Merge** | Transformation (dbt) | ใช้กลยุทธ์ `incremental` ร่วมกับ `merge` ในโมเดลส่วนใหญ่ | ประมวลผลและเขียนเฉพาะ Delta Records ลดทั้งเวลาประมวลผลและค่าเขียนไฟล์บน S3 |
| **Monthly Pre-aggregation** | Serving Layer | ดึงข้อมูลจาก Mart Layer มาทำ Pre-aggregation ให้อยู่ในรูปพร้อมแสดงผลบน Dashboard โดยคำนวณใหม่ทุก 1 เดือน | ลดภาระการประมวลผลและ Query Latency ขณะเปิด Dashboard พร้อมประหยัดต้นทุน Compute จากการคำนวณซ้ำรายวัน |
| **OS Memory Swap** | Orchestration | จัดสรร 4GB Swap Space บนโฮสต์ EC2 | ป้องกัน Linux OOM Killer ปิดการทำงานของ Airflow Scheduler/Worker ระหว่าง Memory Spikes |

## 5. แบบจำลองข้อมูลระดับ Marts (Dimensional Data Model)

สถาปัตยกรรมข้อมูลถูกออกแบบตามหลักการ **Dimensional Modeling (Star Schema with Bridge Tables)** ผ่าน `dbt-athena` โดยอิงตามแนวคิดของ **Ralph Kimball** เพื่อรองรับการวิเคราะห์ข้อมูลภาพยนตร์เชิงลึกที่มีความสัมพันธ์แบบกลุ่มหลายค่า (Many-to-Many Relationships)

### ทำไมต้องใช้ Bridge Tables ใน Data Warehouse?

ตามธรรมชาติของข้อมูลภาพยนตร์ ภาพยนตร์ 1 เรื่องไม่ได้มีเพียงค่าเดียวในแต่ละมิติ เช่น มีได้หลายแนวภาพยนตร์ (Genres), ร่วมทุนผลิตจากหลายประเทศ, และมีหลายคีย์เวิร์ดบ่งบอกธีม (Keywords)

หากออกแบบเป็น Star Schema แบบแบนราบ (Denormalization) โดยไม่ใช้ Bridge Table จะก่อให้เกิดปัญหาหลัก 2 ประการ:

1. **Fact / Dimension Explosion:** เกิดแถวข้อมูลซ้ำซ้อนมหาศาล ทำให้ตารางบวม และเสี่ยงต่อการเกิดปัญหา Double Counting เมื่อรวมยอดสถิติ (Aggregations)
2. **คิวรีวิเคราะห์ได้ยาก:** หากเก็บเป็น List หรือ String รวมในคอลัมน์เดียว จะทำให้เครื่องมือ BI และคำสั่ง SQL คิวรีจัดกลุ่ม (Group by / Filter) ข้ามมิติได้ช้าและไม่มีประสิทธิภาพ

ระบบนี้จึงประยุกต์ใช้เทคนิค [**Multivalued Dimensions and Bridge Tables**](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/multivalued-dimension-bridge-table/) ตามมาตรฐานของ Kimball เพื่อรักษาความบริสุทธิ์ของ Grain ใน Dimension หลัก (`dim_movies`) และแยกความสัมพันธ์ Many-to-Many ออกมาเป็น Bridge Tables อย่างเป็นสัดส่วน

### โครงสร้างตารางใน Data Marts

| Table Name | Layer Type | Grain (ระดับความละเอียดของข้อมูล) | Key Columns & Highlight Features |
| :--- | :--- | :--- | :--- |
| `dim_movies` | Dimension | 1 แถว ต่อ 1 ภาพยนตร์ | Surrogate Key (`movie_sur_id`), ตัวเลขการเงิน (Profit, ROI, Revenue Multiplier) |
| `dim_genres` | Dimension | 1 แถว ต่อ 1 แนวภาพยนตร์ | รองรับ Fallback Default: `-1: None`, `-2: Invalid` |
| `dim_keywords` | Dimension | 1 แถว ต่อ 1 คำสำคัญ (Keyword) | `merge_update_columns = []` เพื่อการทำงานแบบ Insert-only |
| `dim_dates` | Dimension | 1 แถว ต่อ 1 วันในปฏิทิน | Programmatic Generation (2016–2030) ไม่ต้องพึ่งพาไฟล์ CSV ภายนอก |
| `dim_countries` | Dimension | 1 แถว ต่อ 1 ประเทศ | ISO-3166-1 Key พร้อมค่า Default `-1` และ `-2` |
| `bridge_movie_*` | Bridge (M:N) | 1 แถว ต่อ 1 คู่ความสัมพันธ์ | Composite Surrogate Keys, ตัดความซ้ำซ้อนด้วย `UNION`, Insert-only Merge |
| `fact_movie_daily_engagement_30d` | Fact (Periodic) | 1 ภาพยนตร์ ต่อ 1 วันที่วัดผล | **Partitioned by `days_since_release`** รองรับ Retention Curve Analysis |

> 📖 **Reference:** ศึกษาแนวคิดและเทคนิคการจัดการ Multivalued Dimensions เพิ่มเติมได้ที่: [Kimball Group - Multivalued Dimensions and Bridge Tables](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/multivalued-dimension-bridge-table/)

## 6. การนำส่งโค้ดอัตโนมัติ (CI/CD Workflows)

ทำงานผ่าน **GitHub Actions** แยกตาม Path Filters:

* `deploy-lambda-snapshot.yml`: สร้าง Docker Image ขึ้น ECR และอัปเดตฟังก์ชัน Snapshot Lambda
* `deploy-lambda-producer.yml`: สร้าง Docker Image ขึ้น ECR และอัปเดตฟังก์ชัน Producer Lambda
* `deploy-lambda-worker.yml`: สร้าง Docker Image ขึ้น ECR และอัปเดตฟังก์ชัน Worker Lambda
* `deploy-airflow-server.yml`: เชื่อมต่อผ่าน SSH ไปยัง EC2 เพื่อสั่ง `git pull` และ `docker compose up -d --build` เมื่อโค้ดฝั่ง Airflow หรือ dbt มีการแก้ไข

## 7. โครงสร้างโปรเจกต์ (Repository Structure)

```
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
│   │       ├── facts/      # ตาราง Fact วัดกระแส 30 วันหลังหนังฉาย
│   │       └── serving/    # ตารางที่ทำ pre-aggregation จาก fact เพื่อให้ backend ดึงข้อมูลไปแสดงใน dashboardได้อย่างรวดเร็ว
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
* **AWS EC2 & EventBridge:**
  * Ubuntu Server ติดตั้ง Docker, Docker Compose และเปิด 4GB Swap Space
  * ตั้งค่า **Amazon EventBridge Scheduler** สำหรับเปิด-ปิด EC2 ตามรอบเวลาที่กำหนด (ไม่ใช้ Elastic IP เพื่อประหยัดค่าใช้จ่าย)

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

### 8.3 การกำหนด Security & Hybrid CI/CD Configuration

ระบบ Deploy ของ Airflow ถูกออกแบบเป็น **Hybrid CI/CD** เพื่อรองรับ EC2 ที่เปลี่ยน Public IP ทุกครั้งที่เปิดใหม่:

* **เมื่อ Push โค้ดตอน EC2 เปิดอยู่ (`running`):** GitHub Actions จะดึง Public IP ล่าสุดของวันนั้นแบบ Dynamic และ SSH เข้าไปอัปเดตทันที
* **เมื่อ Push โค้ดตอน EC2 ปิดอยู่ (`stopped`):** GitHub Actions จะข้ามการ Deploy (`Exit 0`) โดยไม่ปลุกเครื่องนอกเวลา และปล่อยให้สคริปต์ `@reboot` บน EC2 ดึงโค้ดอัปเดตเองอัตโนมัติในรอบเปิดเครื่องถัดไป

1. **GitHub Secrets:** กำหนดตัวแปรใน `Repository Settings -> Secrets and variables -> Actions`:
   * `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ACCOUNT_ID` *(ต้องมีสิทธิ์ `ec2:DescribeInstances` สำหรับเช็กสถานะและดึง IP ล่าสุด)*
   * `EC2_INSTANCE_ID`: รหัส Instance ID ของเครื่อง EC2 (เช่น `i-0abcd1234efgh5678`)
   * `EC2_AIRFLOW_SERVER_USER`: User ของ EC2 (เช่น `ubuntu`)
   * `EC2_AIRFLOW_SERVER_SSH_KEY`: Private Key (`.pem`) สำหรับเชื่อมต่อ SSH ไปยัง EC2
   * `EC2_AIRFLOW_SERVER_PORT`: Port สำหรับ SSH (เช่น `22`)

2. **GitHub Credentials บน EC2:**
   * ตั้งค่า Deploy Key หรือบันทึก Git Credentials บนเครื่อง EC2 เพื่อให้เซิร์ฟเวอร์สามารถสั่ง `git fetch` และ `git pull` แบบอัตโนมัติได้โดยไม่ติดหน้าถามรหัสผ่าน:

     ```bash
     git config --global credential.helper store
     git fetch origin main
     ```

3. **ตั้งค่า Boot-Time Auto-Deploy Script บน EC2 (`@reboot`):**
   * สร้างไฟล์ `/home/ubuntu/auto_deploy.sh` เพื่อเช็ก Commit ใหม่ทุกครั้งที่เปิดเครื่อง (หากไม่มีโค้ดใหม่จะข้ามทันทีโดยไม่กวนการทำงานของ Airflow):

     ```bash
     cat << 'EOF' > ~/auto_deploy.sh
     #!/bin/bash

     echo "=========================================="
     echo "Boot Deploy Check at: $(date)"
     echo "=========================================="

     sleep 20
     cd /home/ubuntu/movie-data-pipeline || exit 1

     git fetch origin main
     LOCAL_HASH=$(git rev-parse HEAD)
     REMOTE_HASH=$(git rev-parse origin/main)

     if [ "$LOCAL_HASH" = "$REMOTE_HASH" ]; then
       echo "No new code changes ($LOCAL_HASH). Skipping Docker rebuild."
       exit 0
     fi

     echo "New changes detected! Updating from $LOCAL_HASH to $REMOTE_HASH..."
     git pull origin main

     cd airflow || exit 1
     docker compose up -d --build
     echo "Deployment completed at: $(date)"
     EOF

     chmod +x ~/auto_deploy.sh
     ```

   * ผูกสคริปต์เข้ากับ `crontab -e` เพื่อให้ทำงานอัตโนมัติทุกครั้งที่ EC2 เปิดเครื่อง:

     ```cron
     @reboot /home/ubuntu/auto_deploy.sh >> /home/ubuntu/deploy.log 2>&1
     ```

4. **Airflow Environment Configuration:**
   * คัดลอกและตั้งค่าในโฟลเดอร์ `airflow/`:

     ```bash
     cd airflow
     cp .env.example .env
     # กำหนด AIRFLOW_UID=50000 และตั้งค่าการเชื่อมต่อ AWS / Athena
     ```

### 8.4 การรันระบบ (Execution)

#### 8.4.1 การเตรียมข้อมูลย้อนหลัง (Initial Historical Backfill - Optional)

หากต้องการโหลดข้อมูลภาพยนตร์ย้อนหลัง 10 ปี (2016–ปัจจุบัน) เพื่อสร้างชุดข้อมูลเริ่มต้นสำหรับการวิเคราะห์ภาพรวมในระยะยาว สามารถรันสคริปต์เพื่อดึงข้อมูลจาก TMDB API และบันทึกลง Amazon S3 ได้โดยตรง

```bash
# ติดตั้ง Dependencies และรันสคริปต์ผ่าน uv
uv run python scripts/backfill_historical_data.py
```

> **หมายเหตุ:** ขั้นตอนนี้เป็นทางเลือก สามารถข้ามได้หากไม่ต้องการโหลดข้อมูลย้อนหลัง

#### 8.4.2 เริ่มต้นระบบ Orchestration บน EC2

เข้าไปยังโฟลเดอร์ `airflow` และรัน Docker Compose เพื่อเริ่มต้นบริการ Apache Airflow

```bash
cd /home/ubuntu/movie-data-pipeline/airflow

# Build และเริ่มต้น Containers ใน Background
docker compose up -d --build
```

เมื่อระบบเริ่มทำงานแล้ว สามารถเข้าใช้งาน Airflow Web UI ผ่าน Browser ได้ที่:

* **URL:** `http://<EC2_IP>:8088`
* **DAG ID:** `movie_data_pipeline_orchestration`

#### 8.4.3 การรัน dbt Models แบบ Manual (Optional)

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

> **หมายเหตุ:** ขั้นตอนนี้เป็นทางเลือก (ทำหรือไม่ก็ได้) โดยในการทำงานปกติ Apache Airflow จะเป็นผู้เรียกใช้งาน dbt ตามลำดับการทำงานของ Pipeline อยู่แล้ว

---

## EN-Version

An Event-Driven ELT Data Lakehouse built on a Serverless and Container architecture that ingests movie data from the TMDB API, stores it in AWS S3, transforms it into a Dimensional Model (Star Schema) on AWS Athena using dbt Core, orchestrates workflows with Apache Airflow on AWS EC2, and automates Continuous Deployment (CD) via GitHub Actions.

---

## 1. Motivation & Objective

This Automated Data Lakehouse is designed using an Event-Driven architecture and ELT pipeline to ingest, transform, and model movie data from the TMDB API. It supports data analytics across three primary dimensions:
1. **Financial & Box Office Performance:** Covering profitability, revenue distribution, seasonal release windows, and global box office markets.
2. **Narrative & Thematic Insights:** Tracking the evolution of mainstream keyword frequencies and identifying genre-specific uniqueness via TF-IDF calculations.
3. **30-Day Movie Engagement & Audience Behavior Analytics:** Monitoring popularity momentum, cumulative vote growth, rating stability, and hype decay during the first 30 days post-release.

The system follows a **Decoupled Storage & Compute** design on AWS—utilizing Amazon S3 for storage, AWS Glue Data Catalog for metadata management, Amazon Athena for querying, and dbt Core for data transformation and modeling. Pipeline workflows are orchestrated via Apache Airflow, while GitHub Actions handles automated Continuous Deployment (CD).

* **Business Problem:** Investing in the film industry carries high risk. Investment and project development decisions require data-driven insights beyond intuition or post-theatrical gross revenue alone. Key analytical gaps include:
  - **Popularity Trends:** Lack of tools to track and benchmark movie popularity trajectories during the critical first 30 days post-release.
  - **Thematic Content:** Lack of visibility into which themes and keywords dominate across specific years, genres, or countries—insights that help producers spot trending narratives and identify whitespace for differentiated content.
* **Engineering Objective:** Design and build a reliable, scalable, and cost-effective Lakehouse Architecture by decoupling storage and compute. Automate the ELT pipeline via Apache Airflow and GitHub Actions to deliver Analytics-Ready Data powering a Web Dashboard for financial analysis, popularity tracking, and thematic discovery to inform future film development.

---

## 2. Architecture Flow

<img width="1299" height="961" alt="CICD Deployment (5)" src="https://github.com/user-attachments/assets/3e13a0a5-0974-494c-8a34-f15e441d9c4f" />

The entire data pipeline is managed and orchestrated by **Apache Airflow [B]**, deployed as a Docker container on **AWS EC2 [A]**. Airflow serves as the central workflow orchestrator—scheduling runs, managing task dependencies, triggering serverless ingestion, and executing dbt Core models into the Data Lakehouse.

---

### Cost Optimization
* **Scheduled Compute Management (AWS EventBridge [L]):** Automatically starts and stops the **AWS EC2 [A]** instance based on the pipeline schedule to eliminate idle server costs.
* **Serverless Execution (AWS Lambda [C, E, F]):** Utilizes a serverless architecture for data ingestion, eliminating always-on compute overhead and billing strictly per invocation and actual execution duration (GB-seconds). Over nearly a month of continuous daily pipeline runs, Lambda costs remained at **$0**, staying well within the **AWS Always Free Tier** (1,000,000 free requests and 400,000 GB-seconds per month; beyond the free tier, pricing starts at just $0.20 per 1M requests and $0.0000166667 per GB-second).
* **Conditional Monthly Processing (Task 5 & Task 6):** Implements branching logic to check whether the execution date falls on the first day of the month, skipping monthly model runs on regular days to minimize data scanned and reduce **AWS Athena [I]** query costs.

---

### CI/CD Deployment
* **Continuous Deployment to EC2 (GitHub Actions [M] $\rightarrow$ AWS EC2 [A]):** Upon pushing new code to the repository, **GitHub Actions [M]** connects to **AWS EC2 [A]** and pulls the latest codebase (DAGs, dbt models) automatically.
* **Serverless Container Pipeline (GitHub Actions [M] $\rightarrow$ Docker [N] $\rightarrow$ Amazon ECR [O]):** **GitHub Actions [M]** triggers **Docker [N]** to build ingestion function code and dependencies into container images and pushes them to **Amazon ECR [O]**, enabling **AWS Lambda [C, E, F]** functions to deploy the latest image versions.

---

### Data Pipeline Workflow (DAG Tasks)

#### Task 1: `invoke_producer_lambda`
* **Apache Airflow [B]** on **AWS EC2 [A]** triggers **AWS Lambda (Producer) [C]** to initiate the ingestion pipeline.
* **AWS Lambda (Producer) [C]** fetches the list of Daily Changed Movie IDs from the TMDB API.
* Splits the IDs into batches and pushes them to **AWS Simple Queue Service (SQS) [D]**, acting as a buffer and controlling concurrency to stay within API rate limits.
* **AWS Lambda (Workers) [E]** are triggered by **SQS [D]** to fetch detailed movie metadata (e.g., genres, runtime, release date) in parallel (scalable processing) so no single worker is overloaded.
* **AWS Lambda (Workers) [E]** writes the raw JSON payloads into **Amazon S3 (Raw Zone) [G]**.

#### Task 2: `wait_for_workers_completion`
* Pauses execution for 7 minutes to allow **AWS Lambda (Workers) [E]** to poll messages from the queue and complete processing.
* Workers typically finish within 5 minutes; an extra 2-minute buffer accommodates days with unusually high volumes of updated movies.
* Ensures all movie detail API calls and raw JSON writes to **Amazon S3 (Raw Zone) [G]** are fully completed.
* Acts as a synchronization barrier converging with **Task 3**, guaranteeing that both Movie Details and Daily Snapshots are completely landed before starting dbt transformations.

#### Task 3: `invoke_snapshot_lambda`
* Executes in parallel alongside **Task 1**.
* **Apache Airflow [B]** triggers **AWS Lambda [F]**.
* **AWS Lambda [F]** fetches daily audience engagement and popularity metrics (e.g., popularity, vote count, vote average) from the TMDB API for movies released within the past 30 days.
* Writes raw data directly in NDJSON format into **Amazon S3 (Raw Zone) [G]**.

#### Task 4: `trigger_dbt_daily_models`
* **Apache Airflow [B]** executes the **dbt Core [K]** CLI inside the container.
* **dbt Core [K]** compiles SQL models and submits queries via the `dbt-athena-community` adapter to **AWS Athena [I]**.
* **AWS Athena [I]** references table schemas and partitions from **AWS Glue Data Catalog [H]** to query raw JSON files from **Amazon S3 (Raw Zone) [G]**.
* Builds daily dimensional tables (Dimensions), bridge tables (Bridges such as genres and cast), and the 30-day post-release engagement fact table (`fact_movie_daily_engagement_30d`).
* Writes transformed outputs in Parquet format into **Amazon S3 (Gold Zone / Lakehouse) [J]**.

#### Task 5: `evaluate_calendar_boundary`
* Acts as a branching operator checking whether the current run's Execution Date is the **1st of the month**.
* **If No:** Routes to **Task 6: `skip_monthly_transformation`** to immediately conclude the daily pipeline.
* **If Yes:** Routes to **Task 7: `trigger_dbt_monthly_models`** to execute monthly batch processing.

#### Task 6: `skip_monthly_transformation`
* Serves as a dummy task (via `EmptyOperator`) handling non-first-of-month execution paths.
* Terminates the daily pipeline immediately without running monthly models, avoiding unnecessary compute and **AWS Athena [I]** query costs.

#### Task 7: `trigger_dbt_monthly_models`
* **Apache Airflow [B]** triggers **dbt Core [K]** CLI commands for monthly models.
* **dbt Core [K]** instructs **AWS Athena [I]** to perform a full refresh (`materialized='table'`) on `fact_movie_keyword_tfidf` across the entire warehouse.
* **AWS Athena [I]** computes monthly movie summaries, including Monthly Aggregated Metrics, Monthly Popularity Trends, and Monthly Performance Indicators.
* Stores the resulting Parquet files in **Amazon S3 (Gold Zone / Lakehouse) [J]**.

---

## 3. Technical Challenges & Engineering Trade-offs

| Engineering Challenge | The Bottleneck / Failure Mode | Architectural Solution | Trade-off & Mitigation |
|---|---|---|---|
| **500-Page API Limit** | Daily Snapshot ingestion relies on the `/discover/movie` endpoint, which caps pagination at 500 pages—risking data loss during high-volume release windows. | **Window Splitting Strategy:** Split the discovery query into two date windows (`0–14 days` and `15–30 days`), merge results, and deduplicate by `movie_id`. | **Trade-off:** Slightly increases API request volume, but guarantees 100% data completeness. |
| **Ingestion Bottleneck** | Daily changes span thousands to tens of thousands of movies. Sequential fetching with a single worker takes over 30 minutes and risks hitting **Lambda Timeout**, losing in-memory progress and causing full-batch SQS retries. | **Producer-Consumer Pattern:** Use a Producer Lambda to collect Movie IDs and push batches to Amazon SQS, allowing a Lambda Worker Pool to fetch details in parallel. | **Trade-off:** Higher burst throughput eliminates timeouts, requiring careful tuning of message batch sizes, TMDB rate limits, and downstream idempotency. |
| **Barrier Synchronization** | Asynchronous, distributed Lambda Workers prevent Airflow from knowing the exact completion timestamp of all tasks. | **7-Minute Synchronization Delay:** Configure a static 7-minute wait in Airflow (based on a typical <5-minute runtime + 2-minute safety buffer). | **Trade-off:** Adds a 2-minute idle wait, but eliminates the risk of partial data loading before dbt runs. |
| **Server Memory Spikes** | Running Airflow via Docker Compose on EC2 encountered Out-of-Memory (OOM) errors, causing the OS to kill processes. | **OS Swap Memory Tuning:** Allocate 4GB of Swap Space on the Ubuntu host to absorb transient memory spikes. | **Trade-off:** Disk I/O is slower than RAM, but ensures 100% system stability without upgrading to a more expensive EC2 instance. |

---

## 4. ELT Optimization Matrix

| Optimization Technique | Layer | Implementation Details | Quantitative / Technical Impact |
|---|---|---|---|
| **Hive-style Partitioning** | Raw Storage (S3) | Structured raw daily ingestion paths as `year=.../month=.../day=...` and registered partitions in Glue Catalog. | **Reduced Data Scanned by ~87.5%** in Athena (from 98.11 MB down to 12.27 MB in partition pruning benchmarks) by reading only the target day's folder. |
| **Parallel Processing via SQS** | Ingestion | Decoupled Producer and Worker Lambdas using an SQS queue for horizontal scaling. | Cut ingestion runtime from **30+ minutes down to a few minutes**, scaling elastically with daily update volume. |
| **Pre-unnested Bridges** | Data Modeling | Flattened nested arrays (Companies, Genres) into dedicated Bridge Tables during transformation. | Eliminates expensive runtime `CROSS JOIN UNNEST()` operations, enabling millisecond-level dashboard queries. |
| **Incremental Merge** | Transformation (dbt) | Applied `incremental` materialization with `merge` strategy across core models. | Processes and writes only delta records, reducing both execution time and S3 write costs. |
| **Monthly Pre-aggregation** | Serving Layer | Pre-aggregated Mart tables into dashboard-ready serving tables recalculated once a month. | Minimizes query latency and compute overhead when loading dashboard views compared to daily recomputation. |
| **OS Memory Swap** | Orchestration | Configured 4GB Swap Space on the EC2 host. | Prevents the Linux OOM Killer from terminating Airflow Scheduler/Worker containers during peak loads. |

---

## 5. Dimensional Data Model (Marts Layer)

<img width="1822" height="1014" alt="Untitled (31)" src="https://github.com/user-attachments/assets/f9d17427-35ad-4fc9-b704-2b294598d968" />

The data warehouse is modeled following **Dimensional Modeling (Star Schema with Bridge Tables)** principles via `dbt-athena`, grounded in **Ralph Kimball's** methodology to support complex analytical queries across Many-to-Many (M:N) movie attributes.

### Why Use Bridge Tables in a Data Warehouse?
By nature, a single movie has multiple values across several attributes—such as multiple genres, co-producing countries, and thematic keywords.

Flattening these into a standard denormalized Star Schema without bridge tables introduces two major issues:
1. **Fact / Dimension Explosion:** Produces massive row duplication, bloating table storage and risking **double counting** during metric aggregations.
2. **Query Inefficiency:** Storing lists or delimited strings inside a single column degrades BI tool performance and complicates cross-dimensional `GROUP BY` and `FILTER` SQL queries.

To solve this, the model implements Kimball's **[Multivalued Dimensions and Bridge Tables](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/multivalued-dimension-bridge-table/)** pattern—preserving grain integrity in the core dimension (`dim_movies`) while cleanly isolating M:N relationships into dedicated bridge tables.

---

### Data Marts Table Structure

| Table Name | Layer Type | Grain | Key Columns & Highlight Features |
|---|---|---|---|
| `dim_movies` | Dimension | 1 row per movie | Surrogate Key (`movie_sur_id`), financial metrics (Profit, ROI, Revenue Multiplier) |
| `dim_genres` | Dimension | 1 row per genre | Supports fallback defaults: `-1: None`, `-2: Invalid` |
| `dim_keywords` | Dimension | 1 row per keyword | Uses `merge_update_columns = []` for insert-only behavior |
| `dim_dates` | Dimension | 1 row per calendar day | Programmatically generated (2016–2030) with zero external CSV dependencies |
| `dim_countries` | Dimension | 1 row per country | ISO-3166-1 keys with `-1` and `-2` default handling |
| `bridge_movie_*` | Bridge (M:N) | 1 row per relationship pair | Composite surrogate keys, deduplicated via `UNION`, insert-only merge |
| `fact_movie_daily_engagement_30d` | Fact (Periodic) | 1 movie per measurement day | **Partitioned by `days_since_release`** to optimize retention curve analysis |

> 📖 **Reference:** Learn more about handling multivalued dimensions at [Kimball Group - Multivalued Dimensions and Bridge Tables](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/multivalued-dimension-bridge-table/).

---

## 6. Automated CI/CD Workflows

Managed via **GitHub Actions** triggered by path filters:
* `deploy-lambda-snapshot.yml`: Builds the Docker image, pushes it to ECR, and updates the Snapshot Lambda function.
* `deploy-lambda-producer.yml`: Builds the Docker image, pushes it to ECR, and updates the Producer Lambda function.
* `deploy-lambda-worker.yml`: Builds the Docker image, pushes it to ECR, and updates the Worker Lambda function.
* `deploy-airflow-server.yml`: Connects via SSH to EC2 to execute `git pull` and `docker compose up -d --build` whenever Airflow or dbt code changes.

---

## 7. Repository Structure

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
│   │   ├── staging/        # Data cleaning and type casting
│   │   ├── intermediate/   # Financial metric calculations (Profit, ROI)
│   │   └── marts/
│   │       ├── dimensions/ # Dimension tables with surrogate keys
│   │       ├── bridges/    # Bridge tables for M:N relationships
│   │       ├── facts/      # Fact table tracking 30-day post-release engagement
│   │       └── serving/    # Pre-aggregated tables from facts for low-latency dashboard queries
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

### 8.1 Cloud Prerequisites (AWS)
- **Amazon S3:** Data Lakehouse storage bucket
  - `raw/` prefix: Stores raw ingested data strictly in **NDJSON (Newline Delimited JSON)** format
  - `gold/` prefix: Stores transformed Mart-level tables (Parquet format)
- **Amazon ECR:** 3 repositories (`fetcher_for_daily_snapshot`, `producer_for_daily_changes`, `worker_for_daily_changes`)
- **AWS Lambda:** 3 container-backed functions linked to their respective ECR images
- **Amazon SQS:** 1 Standard Queue for buffering Movie ID batches
- **AWS Glue & Athena:** 
  - Create a **Glue Data Catalog Database Schema** matching the raw JSON fields and data types in `raw/` 1:1
  - Create an **Athena Workgroup** configured for `dbt-athena` transformations
- **AWS EC2 & EventBridge:** 
  - Ubuntu Server provisioned with Docker, Docker Compose, and 4GB Swap Space
  - Configure **Amazon EventBridge Scheduler** to start/stop the EC2 instance on schedule (without an Elastic IP to avoid idle IP charges)

### 8.2 AWS Lambda Environment Variables
- **Snapshot Lambda:**
  - `TMDB_API_KEY`: TMDB API key for data extraction
  - `S3_BUCKET_NAME`: Target S3 bucket name
- **Producer Lambda:**
  - `TMDB_API_KEY`: TMDB API key for data extraction
  - `SQS_QUEUE_URL`: Amazon SQS queue URL
- **Worker Lambda:**
  - `TMDB_API_KEY`: TMDB API key for data extraction
  - `S3_BUCKET_NAME`: Target S3 bucket name

### 8.3 Security & Hybrid CI/CD Configuration
Airflow deployment uses a **Hybrid CI/CD** approach designed for an EC2 instance whose Public IP changes on every boot:
- **When pushing code while EC2 is `running`:** GitHub Actions dynamically fetches the instance's current Public IP and deploys updates immediately via SSH.
- **When pushing code while EC2 is `stopped`:** GitHub Actions gracefully skips deployment (`Exit 0`) without waking the instance off-schedule, deferring updates to an `@reboot` script on the next scheduled boot.

1. **GitHub Secrets:** Configure the following under `Repository Settings -> Secrets and variables -> Actions`:
   - `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ACCOUNT_ID` *(requires `ec2:DescribeInstances` permission to check instance state and resolve its dynamic IP)*
   - `EC2_INSTANCE_ID`: Target EC2 Instance ID (e.g., `i-0abcd1234efgh5678`)
   - `EC2_AIRFLOW_SERVER_USER`: EC2 SSH username (e.g., `ubuntu`)
   - `EC2_AIRFLOW_SERVER_SSH_KEY`: Private key (`.pem`) for SSH authentication
   - `EC2_AIRFLOW_SERVER_PORT`: SSH port (e.g., `22`)

2. **GitHub Credentials on EC2:**
   - Configure a Deploy Key or store Git credentials on the EC2 host so `git fetch` and `git pull` run non-interactively:
     ```bash
     git config --global credential.helper store
     git fetch origin main
     ```

3. **Boot-Time Auto-Deploy Script on EC2 (`@reboot`):**
   - Create `/home/ubuntu/auto_deploy.sh` to check for new commits on every boot (exiting immediately if no changes exist so Airflow startup is unaffected):
     ```bash
     cat << 'EOF' > ~/auto_deploy.sh
     #!/bin/bash

     echo "=========================================="
     echo "Boot Deploy Check at: $(date)"
     echo "=========================================="

     sleep 20
     cd /home/ubuntu/movie-data-pipeline || exit 1

     git fetch origin main
     LOCAL_HASH=$(git rev-parse HEAD)
     REMOTE_HASH=$(git rev-parse origin/main)

     if [ "$LOCAL_HASH" = "$REMOTE_HASH" ]; then
       echo "No new code changes ($LOCAL_HASH). Skipping Docker rebuild."
       exit 0
     fi

     echo "New changes detected! Updating from $LOCAL_HASH to $REMOTE_HASH..."
     git pull origin main

     cd airflow || exit 1
     docker compose up -d --build
     echo "Deployment completed at: $(date)"
     EOF

     chmod +x ~/auto_deploy.sh
     ```
   - Register the script in `crontab -e` to execute automatically on boot:
     ```cron
     @reboot /home/ubuntu/auto_deploy.sh >> /home/ubuntu/deploy.log 2>&1
     ```

4. **Airflow Environment Configuration:**
   - Copy and configure the environment file inside `airflow/`:
     ```bash
     cd airflow
     cp .env.example .env
     # Set AIRFLOW_UID=50000 and configure AWS / Athena connection variables
     ```

### 8.4 Execution

#### 8.4.1 Initial Historical Backfill (Optional)

To load 10 years of historical movie data (2016–present) as a baseline dataset for long-term trend analysis, run the backfill script to fetch data from the TMDB API directly into Amazon S3:

```bash
# Install dependencies and run the script via uv
uv run python scripts/backfill_historical_data.py
```

> **Note:** This step is optional and can be skipped if historical baseline data is not required.

#### 8.4.2 Start Orchestration on EC2

Navigate to the `airflow` directory and launch Apache Airflow using Docker Compose:

```bash
cd /home/ubuntu/movie-data-pipeline/airflow

# Build and start containers in detached mode
docker compose up -d --build
```

Once running, access the Airflow Web UI in your browser:

- **URL:** `http://<EC2_IP>:8088`
- **DAG ID:** `movie_data_pipeline_orchestration`

#### 8.4.3 Manual dbt Model Execution (Optional)

To trigger dbt Core transformations manually, run the following commands from `data_transformation_dbt`:

```bash
cd data_transformation_dbt

# Install dbt packages
dbt deps

# Run data transformation models
dbt run

# Validate data quality with tests
dbt test
```

> **Note:** This step is optional; during standard operation, Apache Airflow automatically triggers dbt runs according to the DAG schedule.