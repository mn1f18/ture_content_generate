# True Content服务部署指南

本指南详细说明了如何在服务器上部署True Content服务，并进行日常维护操作。

## 前提条件

- Docker (19.03+)
- Docker Compose (1.27+)
- Git
- 数据库已配置好（MySQL和PostgreSQL）

## 部署说明

本项目依赖于已有的MySQL和PostgreSQL数据库，**数据表已经创建完成，不需要执行创建表的操作**。部署时将直接连接到现有数据库使用现有的表结构。

## 部署步骤

### 1. 克隆代码库

```bash
git clone https://github.com/yourusername/ture_content.git
cd ture_content
```

### 2. 配置环境变量

复制环境变量模板并进行配置：

```bash
cp .env.example .env
vim .env
```

确保以下关键配置项已正确设置：
- MySQL连接信息 (MYSQL_HOST=mcp-mysql-1, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD)
- PostgreSQL连接信息 (PG_HOST=mcp-postgres-1, PG_PORT, PG_USER, PG_PASSWORD)
- 阿里智能体配置 (ALI_AGENT_APP_ID, ALI_AGENT_CONTENT_APP_ID, DASHSCOPE_API_KEY)

### 3. 设置部署脚本权限并执行

```bash
chmod +x setup.sh
./setup.sh
```

部署脚本会自动：
- 检查必要软件依赖
- 创建日志文件
- 确保Docker网络存在
- 构建Docker镜像
- 启动容器服务
- 检查服务是否正常启动

### 4. 验证部署

服务启动后，可以通过以下API验证服务是否正常运行：

```bash
curl http://localhost:5001/api/check_health
```

如果返回状态为`online`，则表示服务已成功部署。

## 日常维护操作

### 查看日志

```bash
# 实时查看应用日志
docker logs -f true-content-api

# 查看最近的日志
docker logs --tail 100 true-content-api

# 查看应用日志文件
cat app.log
```

### 重启服务

```bash
# 重启单个服务
docker restart true-content-api

# 使用docker-compose重启
cd /path/to/ture_content
docker-compose restart
```

### 更新服务

当代码有更新时，按照以下步骤更新服务：

```bash
# 进入项目目录
cd /path/to/ture_content

# 拉取最新代码
git pull

# 重新构建并启动
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

### 手动启动监控服务

部署完成后，可以通过API启动新闻内容自动监控：

```bash
# 启动监控（默认1分钟倒计时）
curl -X POST http://localhost:5001/api/monitor/start

# 自定义倒计时时间（如5分钟）
curl -X POST http://localhost:5001/api/monitor/start -H "Content-Type: application/json" -d '{"minutes": 5}'
```

### 数据库维护

服务依赖于已有的MySQL和PostgreSQL容器，确保这些容器正常运行：

```bash
# 检查数据库容器状态
docker ps | grep "mysql\|postgres"
```

## 故障排除

### 服务无法启动

1. 检查日志查找错误信息：
   ```bash
   docker logs true-content-api
   ```

2. 检查数据库连接是否正常：
   ```bash
   # 通过运行中的容器检查数据库连接
   docker exec -it true-content-api python -c "import mysql.connector; conn=mysql.connector.connect(host='mcp-mysql-1', user='user', password='password', database='news_content'); print('连接成功' if conn else '连接失败')"
   ```

3. 检查环境变量配置是否正确：
   ```bash
   docker exec -it true-content-api env | grep MYSQL
   ```

### 服务运行缓慢

1. 检查资源使用情况：
   ```bash
   docker stats true-content-api
   ```

2. 考虑增加容器资源限制或调整Gunicorn工作进程数量。

## 备份与恢复

### 日志备份

服务使用卷挂载的方式管理日志，日志文件会保存在宿主机上。定期备份：

```bash
# 备份日志文件
cp app.log app.log.backup-$(date +%Y%m%d)
```

## 系统升级

当需要系统组件升级时（如Python版本或依赖库），更新Dockerfile并重新构建：

```bash
# 修改Dockerfile后重新构建
docker-compose build --no-cache
docker-compose up -d
``` 