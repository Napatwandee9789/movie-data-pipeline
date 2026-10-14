# ใช้ Base Image ทางการของ AWS Lambda Python 3.11 หรือ 3.12
FROM public.ecr.aws/lambda/python:3.12

# คัดลอก requirements และติดตั้ง dependencies
COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -r requirements.txt

# คัดลอกโค้ด Lambda
COPY lambda_producer.py ${LAMBDA_TASK_ROOT}

# กำหนด Handler: <ชื่อไฟล์>.<ชื่อฟังก์ชัน>
CMD [ "lambda_producer.lambda_handler" ]