FROM public.ecr.aws/docker/library/python:3.10-slim

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip3 install -r requirements.txt

WORKDIR /app
COPY . /app

RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

CMD ["python3", "app.py"]