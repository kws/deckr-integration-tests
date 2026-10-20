FROM ghcr.io/astral-sh/uv:0.9.5 AS uv
FROM python:3.13.8-slim-bookworm
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /core
COPY --from=core /pyproject.toml /uv.lock /README.md /LICENSE ./
COPY --from=core /src ./src
RUN uv sync --locked --no-dev --extra nats --no-editable
COPY agents/python/worker.py /app/worker.py
COPY protocol /protocol
ENV PROTOCOL_DIR=/protocol PYTHONUNBUFFERED=1
ENTRYPOINT ["/core/.venv/bin/python", "/app/worker.py"]
