# 新闻内容去重系统

该系统用于处理初步爬取的新闻内容，通过智能去重分析将不重复的内容保存到数据库中，以提高后续内容处理的质量。

## 系统架构

系统主要包含以下组件：

1. **数据库结构**：
   - MySQL中存储原始爬取数据(`news_content.step3_content`表)
   - PostgreSQL中存储去重后结果(`true_content_prepare`表)

2. **核心组件**：
   - `deduplication_agent.py` - 去重处理主逻辑，调用阿里智能体进行去重分析
   - `simple_api.py` - 提供简单直观的API接口，支持直接curl调用
   - `monitor_api.py` - 结合API和监控功能，支持自动倒计时处理
   - `create_pg_true_content_prepare.py` - 创建PostgreSQL表结构脚本

## 系统流程

1. 从MySQL数据库中获取新闻数据（按workflow_id分批）
2. 使用阿里百炼智能体进行新闻去重分析
3. 将去重后的数据保存到PostgreSQL数据库

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
DASHSCOPE_API_KEY=your_dashscope_api_key
```

2. 安装依赖：

```bash
pip install -r requirements.txt
```

## 初始化设置

首次使用时，需要创建PostgreSQL数据库表：

```bash
python create_pg_true_content_prepare.py
```

## 使用方法

系统提供两种API服务，可以根据需求选择使用。

### 方式一：简单API服务 (simple_api.py)

适合直接调用处理特定workflow的场景，无监控功能。

```bash
python simple_api.py
```

这将启动一个HTTP服务，默认监听在`http://localhost:5001`。

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

### 方式二：带监控功能的API服务 (monitor_api.py)

包含自动监控和倒计时功能，当检测到新的workflow_id时会自动触发倒计时处理。

```bash
python monitor_api.py
```

通过curl命令调用API：

#### 检查API和监控状态
```bash
curl http://localhost:5001/api/status
```

#### 启动监控（10分钟倒计时）
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

监控模式工作原理：
1. 定期检查MySQL数据库中的最新workflow_id
2. 当检测到新的workflow_id时，开始倒计时
3. 如果在倒计时期间没有检测到新数据更新，倒计时结束后自动处理该workflow
4. 如果期间检测到新的workflow_id，则重置倒计时

这种方式特别适合于需要等待数据更新稳定后再处理的场景，确保处理的数据是完整的。

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

系统使用阿里智能体进行新闻去重，智能体会分析新闻标题和事件标签的相似度，从而识别内容相似的新闻。

智能体接收的输入格式为：
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

智能体返回的JSON结构包含：
- 保留的新闻ID列表（`selected_news`）
- 重复组信息（`duplicate_groups`，包括相似性分析）
- 处理结果摘要（`summary`）

## 性能与优化

* 系统默认只处理重要性为"高"或"中"的新闻，以减少处理量
* 系统会自动跳过已处理过的workflow，避免重复分析

## 故障排除

* 如果遇到数据库连接问题，请检查`.env`文件中的连接配置
* 日志文件（`deduplication.log`、`simple_api.log`和`monitor_api.log`）记录了详细的运行信息，可用于排查问题
* 确保已正确配置阿里智能体的应用ID和API密钥 