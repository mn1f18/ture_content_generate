# 新闻内容去重与优化系统

该系统用于处理初步爬取的新闻内容，通过智能去重分析将不重复的内容保存到数据库中，并通过阿里智能体进行内容优化和审核，以提高内容质量。

## 系统架构

系统主要包含以下组件：

1. **数据库结构**：
   - MySQL中存储原始爬取数据(`news_content.step3_content`表)
   - PostgreSQL中存储去重后的中间结果(`true_content_prepare`表)
   - MySQL中存储优化后的最终内容(`true_content`和`true_content_en`表)

2. **核心组件**：
   - `deduplication_agent.py` - 去重处理主逻辑，调用阿里智能体进行去重分析
   - `process_content_review.py` - 内容审核和优化处理，调用阿里智能体进行内容优化
   - `app.py` - API服务，支持直接处理和自动监控功能
   - `create_pg_true_content_prepare.py` - 创建PostgreSQL表结构脚本
   - `create_true_content_tables.py` - 创建MySQL优化内容表结构脚本

## 系统流程

1. **内容去重阶段**：
   - 从MySQL数据库中获取新闻数据（按workflow_id分批）
   - 使用阿里百炼智能体进行新闻去重分析
   - 将去重后的link_id保存到PostgreSQL数据库

2. **内容优化阶段**：
   - 获取去重后的`link_id`列表
   - 从原始数据表中提取对应内容
   - 调用阿里智能体进行内容审核与优化
   - 将优化后的内容保存到`true_content`(中文)和`true_content_en`(英文)表

## 环境配置

1. 创建`.env`文件，包含以下配置：

```
# MySQL配置
MYSQL_HOST=your_mysql_host
MYSQL_PORT=3306
MYSQL_USER=your_mysql_user
MYSQL_PASSWORD=your_mysql_password
MYSQL_DATABASE=news_content

# PostgreSQL配置
PG_HOST=your_pg_host
PG_PORT=5432
PG_USER=your_pg_user
PG_PASSWORD=your_pg_password
PG_DATABASE=postgres

# 阿里智能体配置
ALI_AGENT_APP_ID=your_ali_agent_app_id
ALI_AGENT_CONTENT_APP_ID=your_content_app_id
DASHSCOPE_API_KEY=your_dashscope_api_key
```

2. 安装依赖：

```bash
pip install -r requirements.txt
```

## 初始化设置

首次使用时，需要创建数据库表：

```bash
# 创建PostgreSQL中去重结果表
python create_pg_true_content_prepare.py

# 创建MySQL中优化内容存储表
python create_true_content_tables.py
```

## 使用方法

系统提供一个统一的API服务，支持直接处理和自动监控功能。

### 启动API服务

```bash
python app.py
```

这将启动一个HTTP服务，默认监听在`http://localhost:5001`。

### API功能

通过curl命令调用API：

#### 检查API状态
```bash
curl http://localhost:5001/api/status
```

#### 获取最新的workflow信息
```bash
curl http://localhost:5001/api/workflows/latest
```

#### 处理最新的workflow
```bash
curl -X POST http://localhost:5001/api/process/latest
```

#### 处理指定的workflow
```bash
curl -X POST http://localhost:5001/api/process/your_workflow_id
```

### 监控功能

系统内置了智能监控功能，可以监控数据库中的新闻条目更新并自动处理：

#### 启动监控（默认1分钟倒计时）
```bash
curl -X POST http://localhost:5001/api/monitor/start
```

#### 设置自定义倒计时（例如5分钟）
```bash
curl -X POST http://localhost:5001/api/monitor/start -H "Content-Type: application/json" -d '{"minutes": 5}'
```

#### 停止监控
```bash
curl -X POST http://localhost:5001/api/monitor/stop
```

### 监控模式工作原理

1. 系统每分钟检查MySQL数据库中的workflow和对应的新闻条目
2. 当检测到新的workflow_id时，记录当前的新闻条目数量
3. 持续监控该workflow_id下的新闻条目数量变化
4. 如果有新的新闻条目添加，重置"无更新"计时器
5. 如果连续10分钟没有新的新闻条目添加，系统认为数据收集已经稳定，开始倒计时（默认1分钟）
6. 倒计时结束后，系统自动执行处理流程：
   - 首先执行去重分析
   - 然后执行内容优化和审核
7. 处理完成后，继续监控新的workflow更新

这种机制确保了：
- 只有当数据收集稳定后（10分钟无新增）才开始处理
- 倒计时期间给予额外缓冲，确保处理的是完整数据
- 自动化执行完整的工作流程，包括去重和内容优化

## Docker部署

系统支持Docker部署，可以使用以下命令构建并运行：

```bash
# 构建Docker镜像
docker build -t news-content-system .

# 运行容器
docker run -d -p 5001:5001 --name news-content-api --env-file .env news-content-system
```

使用Docker Compose更加方便：

```yaml
version: '3'
services:
  news-content-api:
    build: .
    ports:
      - "5001:5001"
    env_file:
      - .env
    restart: always
```

## 自动化使用

对于不需要监控的场景，可以使用系统的计划任务工具：

### Linux/Unix (Cron)
```bash
# 每隔10分钟检查并处理最新的workflow
*/10 * * * * curl -X POST http://localhost:5001/api/process/latest
```

### Windows (计划任务)
可以创建一个批处理文件 (.bat) 并设置为计划任务：
```
curl -X POST http://localhost:5001/api/process/latest
```

## 智能体说明

### 去重智能体

系统使用阿里智能体进行新闻去重，智能体会分析新闻标题和事件标签的相似度，从而识别内容相似的新闻。

去重智能体接收的输入格式为：
```json
{
  "news_list": [
    {
      "link_id": "新闻ID",
      "title": "新闻标题",
      "event_tags": ["事件标签1", "事件标签2"]
    }
    // 更多新闻...
  ]
}
```

去重智能体返回的JSON结构包含：
- 保留的新闻ID列表（`selected_news`）
- 重复组信息（`duplicate_groups`，包括相似性分析）
- 处理结果摘要（`summary`）

### 内容优化智能体

系统使用另一个阿里智能体进行内容优化和审核，该智能体会对内容进行质量评估，优化标题和内容，并提供审核结果。

内容优化智能体接收的输入格式为：
```json
{
  "link_id": "新闻ID",
  "title": "新闻标题",
  "content": "新闻内容",
  "event_tags": ["事件标签1", "事件标签2"],
  "space_tags": ["地区标签1", "地区标签2"],
  "impact_factors": ["影响因素1", "影响因素2"],
  "cat_tags": ["品类标签1", "品类标签2"]
}
```

内容优化智能体返回的JSON结构包含：
- 优化后的标题和内容
- 重要程度评分（importance_score）
- 审核状态（可上架/未通过）
- 审核评价（review_note）
- 英文翻译版本

## 性能与优化

* 系统默认处理重要性为"高"或"中"的新闻，以平衡处理量和质量
* 系统会自动跳过已处理过的workflow，避免重复分析
* 所有数据库操作都增加了重试机制，提高系统稳定性
* 使用连接池和事务控制，确保数据一致性和处理效率
* PostgreSQL连接池配置已优化：
  * 最小连接数（minconn）设置为3，确保足够的空闲连接可用
  * 最大连接数（maxconn）提高到20，增强高负载处理能力
  * 紧急重置阈值从3调整到12，减少不必要的连接池重置
  * 这些优化提高了系统在高并发情况下的稳定性和性能

## 数据库连接管理

系统实现了完善的数据库连接管理机制：

* **自动重试**：所有数据库操作都实现了自动重试机制，最多尝试3次
* **指数退避**：重试间隔时间随着重试次数增加而增长（2的幂次方）
* **连接池监控**：定期检查连接池使用情况，记录统计信息
* **紧急重置**：当PostgreSQL连接池使用率过高时（≥12个连接），自动执行紧急重置
* **连接健康检查**：每10分钟对数据库连接进行健康检查
* **资源回收**：确保所有连接使用后正确归还到连接池或关闭

这些机制大大提高了系统在长时间运行和网络不稳定环境下的可靠性。

## 日志与监控

* 系统自动记录详细的运行日志，包括：
  - 去重处理状态和结果
  - 内容优化处理状态和结果
  - 数据库操作状态
  - 倒计时和监控状态
  - 连接池使用情况和健康状态
* 日志文件保存在`app.log`、`deduplication.log`和`content_review.log`中
* 通过API状态接口随时查看监控状态

## 故障排除

* 如果遇到数据库连接问题，系统会自动尝试重连（最多3次）
* 日志文件记录了详细的运行信息，可用于排查问题
* 确保已正确配置两个阿里智能体的应用ID和API密钥
* 内容优化处理依赖于去重处理的结果，确保先完成去重处理
* 如果频繁出现连接池耗尽问题，考虑进一步增加最大连接数（maxconn）
* 检查数据库服务器配置，确保允许足够的最大连接数 