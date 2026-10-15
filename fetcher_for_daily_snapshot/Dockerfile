# Use AWS Lambda official Python runtime base image
FROM public.ecr.aws/lambda/python:3.12

# Copy requirements specification into the Lambda task root directory
COPY requirements.txt ${LAMBDA_TASK_ROOT}/

# Install application dependencies directly into the system environment
RUN pip install --no-cache-dir -r requirements.txt

# Copy Lambda ingestion logic to task root
COPY lambda_snapshot.py ${LAMBDA_TASK_ROOT}/

# Specify Lambda handler invocation target (<filename>.<handler_function>)
CMD [ "lambda_snapshot.lambda_handler" ]