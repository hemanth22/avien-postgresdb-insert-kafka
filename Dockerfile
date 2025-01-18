# Use an official Python 3.12 runtime as a parent image
FROM python:3.12-slim
#FROM python:3.9
WORKDIR /code
COPY * /code/
RUN chmod -R 777 /code/;pip install --no-cache-dir --upgrade -r /code/requirements.txt
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]