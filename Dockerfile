FROM python:3.9-slim

WORKDIR /app

# 安装curl用于健康检查和其他必要工具
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY *.py .
COPY .env .

# 暴露端口
EXPOSE 5001

# 启动应用
CMD ["gunicorn", "--bind", "0.0.0.0:5001", "--timeout", "1800", "--workers", "2", "app:app"] 