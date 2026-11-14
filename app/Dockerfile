FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

ENV DATA_DIR=/data \
    BATCH_ROWS=20000
VOLUME /data
EXPOSE 8000

# 单 worker + 多线程：合并全局只执行一次（目录库事务保证，跨进程有效），
# 慢下载只占用各自线程，不阻塞后台合并。
# 平滑重载：docker exec <容器> kill -HUP 1 —— 新 worker 启动时旧 worker
# 短暂共存；在途合并由 owner 心跳保护，旧进程退出后新 worker 自动收尾。
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "1", "--threads", "16", \
     "--timeout", "0", "--graceful-timeout", "30", "app:app"]
