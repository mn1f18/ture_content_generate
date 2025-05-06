# 新闻内容去重系统

该系统用于处理初步爬取的新闻内容，通过智能去重分析将不重复的内容保存到数据库中，以提高后续内容处理的质量。

## 系统架构

系统主要包含以下组件：

1. **数据库结构**：
   - MySQL中存储原始爬取数据(`news_content.step3_content`表)
   - PostgreSQL中存储去重后结果(`true_content_prepare`表)

2. **核心组件**：
   - `deduplication_agent.py` - 去重处理主逻辑，调用阿里智能体进行去重分析
   - `app.py` - 提供REST API接口，方便与其他系统集成
   - `timer_monitor.py` - 时间窗口监控程序，实现智能倒计时处理
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

系统提供三种使用方式，可以根据不同场景选择适合的方式。

### 1. API服务

启动API服务：

```bash
python app.py
```

这将启动一个HTTP服务，默认监听在`http://localhost:5001`。

通过curl命令调用API：

**获取最近的workflow列表**

```bash
# 获取最近10个workflow
curl http://localhost:5001/api/workflows

# 获取最近5个workflow
curl http://localhost:5001/api/workflows?limit=5
```

**处理指定workflow**

```bash
# 处理指定的workflow_id
curl -X POST http://localhost:5001/api/process \
  -H "Content-Type: application/json" \
  -d '{"workflow_id": "your_workflow_id"}'

# 处理最新的workflow
curl -X POST http://localhost:5001/api/process \
  -H "Content-Type: application/json" \
  -d '{}'
```

### 2. 直接使用去重脚本

用于单次处理或测试：

```bash
# 处理最新的workflow
python deduplication_agent.py

# 处理指定的workflow_id
python deduplication_agent.py --workflow_id your_workflow_id
```

### 3. 时间窗口监控模式

适用于需要持续监控数据更新的场景：

```bash
python timer_monitor.py --timeout 10
```

参数说明：
- `--timeout`：设置倒计时窗口时间，单位为分钟，默认为10分钟

监控工作原理：
1. 定期检查MySQL数据库中的最新workflow_id
2. 当检测到新的workflow_id时，开始倒计时
3. 在倒计时期间，如果又检测到新数据，则重置倒计时
4. 倒计时结束后，执行去重处理

这种方式非常适合于数据持续更新的场景，可以避免频繁处理，等待数据更新相对稳定后再一次性处理，提高效率。

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
* 使用时间窗口模式可以有效避免频繁处理小批量数据
* 系统会自动跳过已处理过的workflow，避免重复分析

## 故障排除

* 如果遇到数据库连接问题，请检查`.env`文件中的连接配置
* 日志文件（`deduplication.log`、`api.log`和`timer_monitor.log`）记录了详细的运行信息，可用于排查问题
* 确保已正确配置阿里智能体的应用ID和API密钥 