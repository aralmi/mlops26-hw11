ARG PYTHON_IMAGE=python:3.12.11-slim-bookworm@sha256:519591d6871b7bc437060736b9f7456b8731f1499a57e22e6c285135ae657bf7
FROM ${PYTHON_IMAGE}
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --require-hashes -r requirements.txt
COPY api.py simulator.py ./
COPY scripts/load.py /app/load.py
USER 65532:65532
EXPOSE 8080
CMD ["uvicorn","api:app","--host","0.0.0.0","--port","8080","--timeout-graceful-shutdown","25","--no-access-log"]
