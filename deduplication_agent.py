import mysql.connector
import psycopg2
import json
import logging
import os
import time
import argparse
from datetime import datetime
from dotenv import load_dotenv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dashscope import Application

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("deduplication.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DeduplicationAgent")

# MySQL配置
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST'),
    'port': int(os.getenv('MYSQL_PORT')),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE')
}

# PostgreSQL配置
PG_CONFIG = {
    'host': os.getenv('PG_HOST'),
    'port': int(os.getenv('PG_PORT')),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD'),
    'dbname': os.getenv('PG_DATABASE')
}

# 阿里智能体配置
ALI_AGENT_APP_ID = os.getenv('ALI_AGENT_APP_ID')
DASHSCOPE_API_KEY = os.getenv('DASHSCOPE_API_KEY')

def get_latest_workflow_id():
    """获取最新的workflow_id"""
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT workflow_id 
    FROM news_content.step3_content 
    WHERE state LIKE '%爬取成功%'
    GROUP BY workflow_id
    ORDER BY MAX(created_at) DESC 
    LIMIT 1
    """
    
    cursor.execute(query)
    result = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    if result:
        return result['workflow_id']
    return None

def get_news_by_workflow(workflow_id):
    """获取指定workflow_id的所有重要新闻"""
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT link_id, title, event_tags
    FROM news_content.step3_content 
    WHERE workflow_id = %s
    AND state LIKE '%爬取成功%'
    AND importance IN ('高', '中')
    """
    
    cursor.execute(query, (workflow_id,))
    news_items = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    # 处理JSON字段
    for item in news_items:
        if isinstance(item['event_tags'], str):
            try:
                item['event_tags'] = json.loads(item['event_tags'])
            except json.JSONDecodeError:
                item['event_tags'] = []
    
    return news_items

def extract_json_from_text(text):
    """从文本中提取JSON内容"""
    try:
        # 查找第一个{和最后一个}之间的内容
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1:
            json_str = text[start:end+1]
            return json.loads(json_str)
        return None
    except json.JSONDecodeError as e:
        logger.error(f"JSON解析失败: {str(e)}, 文本: {text[:200]}...")
        return None

def call_ali_agent(news_data):
    """调用阿里智能体进行去重分析"""
    if not DASHSCOPE_API_KEY:
        logger.warning("未配置阿里智能体API密钥，使用模拟数据")
        return mock_deduplication_result(news_data)
    
    # 准备输入数据
    news_list = []
    for news in news_data:
        news_list.append({
            'link_id': news['link_id'],
            'title': news['title'],
            'event_tags': news['event_tags']
        })
    
    # 将新闻列表转换为JSON字符串
    input_data = json.dumps({"news_list": news_list}, ensure_ascii=False)
    
    try:
        logger.info(f"开始调用阿里智能体，新闻数量: {len(news_list)}")
        
        # 调用阿里百炼应用
        response = Application.call(
            api_key=DASHSCOPE_API_KEY,
            app_id=ALI_AGENT_APP_ID,
            prompt=input_data
        )
        
        if response.status_code == 200:
            logger.info("阿里智能体调用成功")
            # 从响应中提取JSON
            result = extract_json_from_text(response.output.text)
            if result:
                return result
            else:
                logger.error(f"无法从响应中提取JSON: {response.output.text[:200]}...")
                return None
        else:
            logger.error(f"阿里智能体调用失败: {response.status_code}, {response.message}")
            return None
            
    except Exception as e:
        logger.error(f"调用阿里智能体时出错: {str(e)}")
        return None

def mock_deduplication_result(news_data):
    """生成模拟的去重结果（仅在没有API密钥时使用）"""
    if len(news_data) <= 1:
        return {
            "selected_news": [{"link_id": news["link_id"]} for news in news_data],
            "duplicate_groups": [],
            "summary": {
                "total_input": len(news_data),
                "unique_kept": len(news_data),
                "duplicate_found": 0,
                "duplicate_groups_count": 0
            }
        }
    
    # 简单模拟：假设每3条新闻中有1条是重复的
    selected_news = []
    duplicate_groups = []
    
    for i in range(0, len(news_data), 3):
        group = news_data[i:i+3]
        if len(group) > 1:
            # 保留第一条，其余标记为重复
            kept = group[0]
            dups = group[1:]
            
            selected_news.append({"link_id": kept["link_id"]})
            
            duplicate_groups.append({
                "kept_id": kept["link_id"],
                "duplicates": [d["link_id"] for d in dups],
                "similarity_notes": f"模拟去重：保留{kept['link_id']}，移除{len(dups)}个重复项"
            })
        else:
            # 只有一条，直接保留
            selected_news.append({"link_id": group[0]["link_id"]})
    
    return {
        "selected_news": selected_news,
        "duplicate_groups": duplicate_groups,
        "summary": {
            "total_input": len(news_data),
            "unique_kept": len(selected_news),
            "duplicate_found": len(news_data) - len(selected_news),
            "duplicate_groups_count": len(duplicate_groups)
        }
    }

def save_to_postgres(dedup_result, workflow_id):
    """将去重结果保存到PostgreSQL数据库"""
    if not dedup_result or 'selected_news' not in dedup_result:
        logger.error("去重结果无效，无法保存")
        return False
    
    # 创建重复组映射表，快速查找每个link_id的相似性笔记
    similarity_notes_map = {}
    if 'duplicate_groups' in dedup_result:
        for group in dedup_result['duplicate_groups']:
            kept_id = group.get('kept_id')
            notes = group.get('similarity_notes', '')
            
            if kept_id:
                similarity_notes_map[kept_id] = notes
    
    conn = None
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # 保存选中的新闻到PostgreSQL
        for news in dedup_result['selected_news']:
            link_id = news.get('link_id')
            if not link_id:
                continue
                
            notes = similarity_notes_map.get(link_id, '')
            
            # 插入或更新记录
            insert_query = """
            INSERT INTO true_content_prepare 
            (link_id, workflow_id, similarity_notes, created_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (link_id) 
            DO UPDATE SET 
                workflow_id = EXCLUDED.workflow_id,
                similarity_notes = EXCLUDED.similarity_notes,
                created_at = NOW()
            """
            
            cursor.execute(insert_query, (link_id, workflow_id, notes))
        
        # 记录处理结果
        summary = dedup_result.get('summary', {})
        logger.info(f"保存到PostgreSQL: 总计{summary.get('total_input', 0)}条，"
                   f"保留{summary.get('unique_kept', 0)}条，"
                   f"发现{summary.get('duplicate_found', 0)}条重复")
        
        cursor.close()
        return True
        
    except Exception as e:
        logger.error(f"保存到PostgreSQL失败: {str(e)}")
        return False
    finally:
        if conn:
            conn.close()

def workflow_exists_in_pg(workflow_id):
    """检查指定workflow_id是否已在PostgreSQL中处理过"""
    conn = None
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        query = """
        SELECT COUNT(*) FROM true_content_prepare
        WHERE workflow_id = %s
        """
        
        cursor.execute(query, (workflow_id,))
        count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return count > 0
    except Exception as e:
        logger.error(f"检查workflow_id异常: {str(e)}")
        if conn:
            conn.close()
        return False

def process_workflow(workflow_id=None):
    """处理指定工作流程，如果未指定则处理最新的工作流程"""
    # 如果未指定workflow_id，获取最新的
    if not workflow_id:
        workflow_id = get_latest_workflow_id()
        if not workflow_id:
            logger.warning("未找到有效的workflow_id")
            return False
    
    logger.info(f"开始处理workflow_id: {workflow_id}")
    
    # 检查是否已经处理过
    if workflow_exists_in_pg(workflow_id):
        logger.info(f"workflow_id {workflow_id} 已经处理过，跳过")
        return True
    
    # 获取该workflow的新闻数据
    news_data = get_news_by_workflow(workflow_id)
    if not news_data:
        logger.warning(f"workflow_id {workflow_id} 没有找到任何高重要性的新闻")
        return False
    
    logger.info(f"获取到 {len(news_data)} 条新闻，准备进行去重处理")
    
    # 调用阿里智能体进行去重
    dedup_result = call_ali_agent(news_data)
    if not dedup_result:
        logger.error("调用智能体失败，无法完成去重")
        return False
    
    # 将结果保存到PostgreSQL
    success = save_to_postgres(dedup_result, workflow_id)
    
    return success

def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='处理新闻去重任务')
    parser.add_argument('--workflow_id', type=str, help='指定要处理的workflow_id，不指定则处理最新的')
    args = parser.parse_args()
    
    logger.info("开始运行新闻去重处理")
    
    try:
        success = process_workflow(args.workflow_id)
        if success:
            logger.info("新闻去重处理完成")
            print("新闻去重处理完成")
        else:
            logger.warning("新闻去重处理未完全成功")
            print("新闻去重处理未完全成功")
    except Exception as e:
        logger.error(f"处理过程中出现异常: {str(e)}")
        print(f"处理过程中出现异常: {str(e)}")
    
    logger.info("程序执行结束")

if __name__ == "__main__":
    main() 