FROM python:3.14-slim

ARG TARGETARCH

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# install build toolchain only for arm64/aarch64 source builds because no prebuilt wheel of aiotieba for linux-arm64
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && if [ "$TARGETARCH" = "arm64" ]; then \
        apt-get install -y --no-install-recommends build-essential gcc g++ cmake; \
    fi \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY main.py ./
COPY tables.py ./
COPY utils.py ./
COPY lib.py ./
COPY web_get_tids_reply_order.py ./

CMD ["uv", "run", "python", "main.py"]
