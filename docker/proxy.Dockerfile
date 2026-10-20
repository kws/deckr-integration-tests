FROM python:3.13.8-slim-bookworm
COPY scripts/proxy.py /app/proxy.py
ENTRYPOINT ["python", "/app/proxy.py"]
