#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
from logging.handlers import RotatingFileHandler

# 加载环境变量
load_dotenv()

# 配置日志
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler = RotatingFileHandler(
    "logs/app.log", 
    maxBytes=5*1024*1024,  # 5MB
    backupCount=3,
    encoding='utf-8'
)
log_handler.setFormatter(log_formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

# 确保删除已存在的处理器，避免重复添加
logger = logging.getLogger("DeduplicationAgent")
logger.setLevel(logging.INFO)
if logger.handlers:
    logger.handlers.clear()
logger.addHandler(log_handler)
logger.addHandler(console_handler)
logger.propagate = False  # 防止日志传播到根日志器

# 设置第三方库日志级别为WARNING，减少噪音
logging.getLogger("mysql.connector").setLevel(logging.WARNING)
logging.getLogger("dashscope").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

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

# 导入dashscope - 修改导入方式适应新版API
try:
    from dashscope import Generation
    use_generation_api = True
except ImportError:
    try:
        from dashscope import Application
        use_generation_api = False
    except ImportError:
        from dashscope.api import call
        use_generation_api = False
        print("使用基础dashscope.api.call方法")

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
    """从文本中提取JSON部分"""
    import re
    
    # 尝试提取 {} 闭合的部分
    if not text:
        return None
        
    # 尝试多种JSON提取方法
    json_pattern = r'({[\s\S]*})'
    matches = re.findall(json_pattern, text)
    
    for match in matches:
        try:
            # 尝试解析这个匹配的片段
            test_json = json.loads(match)
            # 如果能解析成功并包含目标字段，则返回
            if 'selected_news' in test_json:
                return match
        except:
            continue
    
    # 如果未找到有效的JSON字符串，记录错误并返回None
    logger.warning(f"无法从文本中提取有效的JSON: {text[:200]}...")
    return None

def get_deduplicated_news_ids(news_list):
    """调用阿里去重智能体获取去重结果
    
    Args:
        news_list: 原始新闻列表
        
    Returns:
        selected_news: 去重后保留的新闻ID列表
    """
    # 准备输入数据
    input_data = {
        "news_list": news_list
    }
    
    logger.info(f"调用阿里智能体进行新闻去重，共 {len(news_list)} 条")
    
    try:
        # 使用不同的API调用方式
        if use_generation_api:
            response = Generation.call(
                model=ALI_AGENT_APP_ID,
                api_key=DASHSCOPE_API_KEY,
                prompt=json.dumps(input_data, ensure_ascii=False)
            )
            if response.status_code == 200:
                result = response.output.text
            else:
                logger.error(f"调用阿里智能体失败: {response.message}")
                return None
        else:
            try:
                # 尝试使用Application类
                response = Application.call(
                    api_key=DASHSCOPE_API_KEY,
                    app_id=ALI_AGENT_APP_ID,
                    prompt=json.dumps(input_data, ensure_ascii=False)
                )
                if response['code'] == 'success':
                    result = response['output']['text']
                else:
                    logger.error(f"调用阿里智能体失败: {response.get('message', '未知错误')}")
                    return None
            except NameError:
                # 使用基础call方法
                response = call(
                    'aigc',
                    api_key=DASHSCOPE_API_KEY,
                    app_id=ALI_AGENT_APP_ID,
                    prompt=json.dumps(input_data, ensure_ascii=False)
                )
                if response['code'] == 'success':
                    result = response['output']['text']
                else:
                    logger.error(f"调用阿里智能体失败: {response.get('message', '未知错误')}")
                    return None
        
        # 提取JSON结果
        selected_news = extract_selected_news(result)
        
        logger.info(f"阿里智能体去重完成，保留 {len(selected_news) if selected_news else 0} 条")
        return selected_news
        
    except Exception as e:
        logger.error(f"调用阿里智能体过程中发生错误: {str(e)}")
        return None

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
        # 抛出异常，确保数据库连接失败时不会继续处理
        raise Exception(f"无法检查workflow_id是否存在: {str(e)}")

def process_workflow(workflow_id=None):
    """处理指定工作流程，如果未指定则处理最新的工作流程"""
    # 如果未指定workflow_id，获取最新的
    if not workflow_id:
        workflow_id = get_latest_workflow_id()
        if not workflow_id:
            logger.warning("未找到有效的workflow_id")
            return False
    
    logger.info(f"开始处理workflow_id: {workflow_id}")
    
    try:
        # 首先检查是否已经处理过，避免重复处理
        if workflow_exists_in_pg(workflow_id):
            logger.info(f"workflow_id {workflow_id} 已经存在于PostgreSQL中，跳过处理")
            return True
        
        # 获取该workflow的新闻数据
        news_data = get_news_by_workflow(workflow_id)
        if not news_data:
            logger.warning(f"workflow_id {workflow_id} 没有找到任何符合条件的新闻")
            return False
        
        logger.info(f"获取到 {len(news_data)} 条新闻，准备进行去重处理")
        
        # 调用阿里智能体进行去重
        dedup_result = get_deduplicated_news_ids(news_data)
        if not dedup_result:
            logger.error("调用智能体失败，无法完成去重")
            return False
        
        # 将结果保存到PostgreSQL
        success = save_to_postgres(dedup_result, workflow_id)
        if success:
            logger.info(f"成功将去重结果保存到PostgreSQL，workflow_id: {workflow_id}")
        
        return success
    except Exception as e:
        logger.error(f"处理workflow过程中出现异常: {str(e)}")
        return False

def extract_selected_news(text):
    """从智能体响应中提取选中的新闻ID列表
    
    Args:
        text: 智能体响应文本
        
    Returns:
        list: 选中的新闻ID列表
    """
    try:
        # 尝试直接解析
        if isinstance(text, str):
            try:
                result = json.loads(text)
            except json.JSONDecodeError:
                # 尝试从文本中提取JSON部分
                text = extract_json_from_text(text)
                if text:
                    result = json.loads(text)
                else:
                    logger.error("无法从响应中提取JSON")
                    return None
        else:
            result = text  # 已经是JSON对象
        
        # 验证结果格式
        if 'selected_news' in result:
            return result['selected_news']
        else:
            logger.error("响应中没有selected_news字段")
            logger.debug(f"响应内容: {json.dumps(result, ensure_ascii=False)[:500]}...")
            return None
    except Exception as e:
        logger.error(f"解析智能体响应时出错: {str(e)}")
        return None

def call_ali_agent(news_data):
    """调用阿里智能体进行去重分析"""
    if not DASHSCOPE_API_KEY or not ALI_AGENT_APP_ID:
        logger.error("阿里智能体API密钥或应用ID未配置，无法进行去重处理")
        return None
    
    # 准备输入数据
    news_list = []
    for news in news_data:
        news_list.append({
            'link_id': news['link_id'],
            'title': news['title'],
            'event_tags': news['event_tags']
        })
    
    # 调用通用函数获取去重结果
    result = get_deduplicated_news_ids(news_list)
    
    if result:
        # 构建完整的返回结果格式
        return {'selected_news': result}
    else:
        return None

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