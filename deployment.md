# True Content 系统部署指南

## 部署架构

本系统采用Docker容器化部署，包括以下组件：

1. **MySQL容器** - 存储原始数据和优化后的内容
2. **PostgreSQL容器** - 存储去重后的中间数据
3. **True Content应用容器** - 处理内容去重和优化
4. **Nginx容器** - 反向代理，提供统一访问入口

这些服务共享一个Docker网络，实现容器间的通信。

## 部署步骤

### 1. 准备环境文件

首先确保已创建`.env`文件，包含必要的环境变量：

```
# MySQL配置
MYSQL_HOST=mysql
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=root_password
MYSQL_DATABASE=news_content

# PostgreSQL配置
PG_HOST=postgres
PG_PORT=5432
PG_USER=postgres
PG_PASSWORD=root_password
PG_DATABASE=postgres

# 阿里智能体配置
ALI_AGENT_APP_ID=47d08f89417a4a538bc822b6379e7cf8
ALI_AGENT_CONTENT_APP_ID=cef2a8954a144d0c935453f76849eec5
ALI_AGENT_CONTENT_EN_APP_ID=0336fde2b8bf4d9492d1fe85f5211137
DASHSCOPE_API_KEY=sk-241d5290235f4979b4787a69cd575b9b
```

### 2. 准备Docker Compose配置

创建`docker-compose.yml`文件：

```yaml
version: '3'

services:
  true_content:
    build: .
    container_name: true_content
    restart: always
    ports:
      - "5001:5001"
    volumes:
      - ./app.log:/app/app.log
    env_file:
      - .env
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:5001/api/check_health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s
    networks:
      - mcp-network

networks:
  mcp-network:
    external: true
```

### 3. 准备Dockerfile

创建`Dockerfile`文件：

```Dockerfile
FROM python:3.9-slim

WORKDIR /app

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY *.py .
COPY .env .

# 初始化表结构
RUN python create_true_content_tables.py

# 暴露端口
EXPOSE 5001

# 启动应用
CMD ["gunicorn", "--bind", "0.0.0.0:5001", "--timeout", "120", "--workers", "2", "app:app"]
```

### 4. 准备Nginx配置

将`nginx/default.conf`文件复制到服务器上，替换现有的Nginx配置：

```bash
# 将配置文件复制到Nginx容器
docker cp nginx/default.conf mcp-nginx-1:/etc/nginx/conf.d/default.conf

# 重新加载Nginx配置
docker exec mcp-nginx-1 nginx -s reload
```

### 5. 构建和启动服务

```bash
# 构建并启动True Content服务
docker-compose up -d --build true_content
```

### 6. 验证部署

部署完成后，可以通过以下URL访问服务：

- True Content API: `http://服务器IP/api/status`
- True Content 健康检查: `http://服务器IP/true_content_health`

## 维护和管理

### 查看日志

```bash
# 查看True Content服务日志
docker logs true_content

# 查看Nginx日志
docker logs mcp-nginx-1
```

### 重启服务

```bash
# 重启True Content服务
docker-compose restart true_content

# 重启Nginx服务
docker restart mcp-nginx-1
```

### 更新服务

当代码有更新时，执行以下步骤更新服务：

```bash
# 拉取最新代码
git pull

# 重新构建并启动服务
docker-compose up -d --build true_content

# 如果Nginx配置有变更，需要重新加载
docker cp nginx/default.conf mcp-nginx-1:/etc/nginx/conf.d/default.conf
docker exec mcp-nginx-1 nginx -s reload
```

## 故障排除

### 数据库连接问题

如果遇到数据库连接问题，检查：

1. 环境变量配置是否正确
2. MySQL和PostgreSQL容器是否正常运行
3. 网络连接是否正常

### API不可访问

如果API无法访问，检查：

1. True Content容器是否正常运行 (`docker ps`)
2. Nginx配置是否正确
3. 服务网络配置是否正确

### 性能问题

如果系统响应缓慢，考虑：

1. 增加Gunicorn工作进程数量
2. 优化数据库查询
3. 检查服务器资源使用情况 