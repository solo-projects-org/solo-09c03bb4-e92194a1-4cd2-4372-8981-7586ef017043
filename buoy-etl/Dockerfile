# Web 镜像：FastAPI + PyArrow（对象源是纯标准库，复用同一基础镜像即可）
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# 先装依赖，利用层缓存
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# 业务代码
COPY cache /app/cache
COPY web /app/web
COPY object_source /app/object_source
COPY scripts /app/scripts

# 非 root 运行；/data 由 compose 挂载并赋权
RUN useradd --create-home --uid 10001 appuser && mkdir -p /data && chown -R appuser:appuser /data /app
USER appuser

EXPOSE 8000
CMD ["uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8000"]
