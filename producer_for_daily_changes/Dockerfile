# Base image: Official AWS Lambda Python 3.12 (includes Lambda Runtime Interface Client)
FROM public.ecr.aws/lambda/python:3.12

# Copy dependencies first to leverage Docker layer caching during rebuilds
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install packages with --no-cache-dir to reduce final image size and Lambda cold starts
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code into Lambda execution directory (/var/task)
COPY lambda_producer.py ${LAMBDA_TASK_ROOT}

# Define Lambda function entrypoint (<module>.<handler_function>)
CMD [ "lambda_producer.lambda_handler" ]