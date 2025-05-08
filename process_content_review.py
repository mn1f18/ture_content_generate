#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import logging
import time
import mysql.connector
import psycopg2
import requests
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
from datetime import datetime
from dashscope import Application  # 添加import

# 配置日志
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler = RotatingFileHandler(
    "content_review.log", 
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5,
    encoding='utf-8'
)
log_handler.setFormatter(log_formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

logger = logging.getLogger("ContentReview")
logger.setLevel(logging.INFO)
if logger.handlers:
    logger.handlers.clear()
logger.addHandler(log_handler)
logger.addHandler(console_handler)
logger.propagate = False

# 加载环境变量
load_dotenv()

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

# 阿里云智能体配置
ALI_AGENT_CONTENT_APP_ID = os.getenv('ALI_AGENT_CONTENT_APP_ID')
DASHSCOPE_API_KEY = os.getenv('DASHSCOPE_API_KEY')
API_URL = f"https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"

# 更新为使用qwen-plus模型
MODEL_NAME = "qwen-plus"  # 之前使用的是qwen-max

def get_deduplicated_link_ids(workflow_id, max_retries=3):
    """
    从PostgreSQL的true_content_prepare表中获取去重后的link_id列表，添加重试机制
    """
    retry_count = 0
    while retry_count < max_retries:
        conn = None
        try:
            conn = psycopg2.connect(**PG_CONFIG)
            cursor = conn.cursor()
            
            query = """
            SELECT link_id FROM true_content_prepare 
            WHERE workflow_id = %s
            """
            
            cursor.execute(query, (workflow_id,))
            results = cursor.fetchall()
            
            link_ids = [row[0] for row in results]
            logger.info(f"从PostgreSQL获取到{len(link_ids)}条去重后的link_id，workflow_id: {workflow_id}")
            
            return link_ids
        except psycopg2.OperationalError as e:
            retry_count += 1
            wait_time = 2 * retry_count  # 指数退避
            logger.warning(f"PostgreSQL连接错误，尝试重试 ({retry_count}/{max_retries}): {str(e)}, 等待{wait_time}秒")
            time.sleep(wait_time)
        except Exception as e:
            logger.error(f"从PostgreSQL获取link_id失败: {str(e)}")
            return []
        finally:
            if conn:
                try:
                    cursor.close()
                    conn.close()
                except:
                    pass
    
    logger.error(f"获取workflow_id {workflow_id} 的link_id列表失败，已达到最大重试次数")
    return []

def get_original_content(link_id, max_retries=3):
    """
    从MySQL的step3_content表中获取原始内容，添加重试机制
    """
    retry_count = 0
    while retry_count < max_retries:
        conn = None
        try:
            conn = mysql.connector.connect(**MYSQL_CONFIG)
            cursor = conn.cursor(dictionary=True)
            
            query = """
            SELECT 
                link_id, title, content, 
                event_tags, space_tags, cat_tags, impact_factors,
                publish_time, importance, source_note, homepage_url, workflow_id
            FROM news_content.step3_content 
            WHERE link_id = %s
            AND state LIKE '%爬取成功%'
            """
            
            cursor.execute(query, (link_id,))
            result = cursor.fetchone()
            
            if result:
                # 转换JSON字符串为Python列表
                for tag_field in ['event_tags', 'space_tags', 'cat_tags', 'impact_factors']:
                    if result[tag_field]:
                        try:
                            result[tag_field] = json.loads(result[tag_field])
                        except json.JSONDecodeError:
                            result[tag_field] = []
                    else:
                        result[tag_field] = []
                
                logger.info(f"成功获取link_id为{link_id}的原始内容")
                return result
            else:
                logger.warning(f"未找到link_id为{link_id}的内容")
                return None
        except mysql.connector.Error as e:
            retry_count += 1
            wait_time = 2 * retry_count  # 指数退避
            logger.warning(f"MySQL连接错误，尝试重试 ({retry_count}/{max_retries}): {str(e)}, 等待{wait_time}秒")
            time.sleep(wait_time)
        except Exception as e:
            logger.error(f"从MySQL获取原始内容失败: {str(e)}")
            return None
        finally:
            if conn:
                try:
                    cursor.close()
                    conn.close()
                except:
                    pass
    
    logger.error(f"获取link_id为{link_id}的原始内容失败，已达到最大重试次数")
    return None

def call_ali_agent(article_data):
    """
    调用阿里云智能体进行内容审核和优化
    """
    if not DASHSCOPE_API_KEY or not ALI_AGENT_CONTENT_APP_ID:
        logger.error("阿里智能体API密钥或应用ID未配置，无法进行内容审核")
        return None
    
    # 准备输入数据
    input_data = json.dumps({
        "article": {
            "link_id": article_data["link_id"],
            "title": article_data["title"],
            "content": article_data["content"],
            "event_tags": article_data["event_tags"],
            "space_tags": article_data["space_tags"],
            "cat_tags": article_data["cat_tags"],
            "impact_factors": article_data["impact_factors"]
        }
    }, ensure_ascii=False)
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            logger.info(f"开始调用阿里智能体进行内容审核，link_id: {article_data['link_id']}")
            
            # 调用阿里百炼应用，与deduplication_agent.py保持一致
            response = Application.call(
                api_key=DASHSCOPE_API_KEY,
                app_id=ALI_AGENT_CONTENT_APP_ID,
                prompt=input_data
            )
            
            if response.status_code == 200:
                logger.info(f"阿里智能体调用成功，link_id: {article_data['link_id']}")
                
                # 从响应中提取JSON
                try:
                    # 先尝试从text中提取JSON
                    if hasattr(response.output, 'text'):
                        result_text = response.output.text
                        # 查找JSON部分
                        result = extract_json_from_text(result_text)
                        if result and 'review_result' in result:
                            return result['review_result']
                    # 如果不存在text属性，尝试直接获取
                    if hasattr(response, 'output'):
                        if isinstance(response.output, dict):
                            return response.output.get('review_result')
                    
                    logger.error(f"找不到review_result字段，响应格式不正确: {str(response)[:500]}...")
                    return None
                except Exception as e:
                    logger.error(f"解析响应时出错: {str(e)}")
                    return None
            else:
                logger.error(f"阿里智能体调用失败: 状态码={response.status_code}, 消息={response.message}")
                retry_count += 1
                time.sleep(2)  # 等待2秒后重试
                
        except Exception as e:
            logger.error(f"调用阿里智能体时出错: {str(e)}")
            retry_count += 1
            time.sleep(2)  # 等待2秒后重试
    
    logger.error(f"调用阿里智能体失败，已达到最大重试次数")
    return None

def extract_json_from_text(text):
    """从文本中提取JSON内容，支持处理带有Markdown代码块格式的JSON"""
    try:
        # 检查是否是Markdown格式的JSON代码块
        if "```json" in text:
            # 移除Markdown代码块标记
            start = text.find("```json") + 7  # 7是```json的长度
            end = text.rfind("```")
            if end > start:  # 确保找到了结束标记
                json_str = text[start:end].strip()
                return json.loads(json_str)
            
        # 如果不是Markdown格式，尝试常规提取
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1:
            json_str = text[start:end+1]
            return json.loads(json_str)
        
        # 如果以上方法都失败，尝试直接解析整个文本
        return json.loads(text)
            
    except json.JSONDecodeError as e:
        logger.error(f"JSON解析失败: {str(e)}, 文本: {text[:200]}...")
        # 尝试清理文本后再解析
        try:
            # 移除可能的非JSON字符
            clean_text = ''.join(c for c in text if c in '{}[]()":,0123456789.abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_ -\n\t')
            start = clean_text.find('{')
            end = clean_text.rfind('}')
            if start != -1 and end != -1:
                json_str = clean_text[start:end+1]
                return json.loads(json_str)
        except:
            pass
        return None

def save_to_true_content(review_result, original_data, max_retries=3):
    """
    将审核结果保存到true_content表，添加重试机制
    """
    retry_count = 0
    while retry_count < max_retries:
        conn = None
        try:
            conn = mysql.connector.connect(**MYSQL_CONFIG)
            cursor = conn.cursor()
            
            # 准备插入的数据
            insert_query = """
            INSERT INTO news_content.true_content (
                link_id, title, content, 
                event_tags, space_tags, impact_factors, cat_tags,
                publish_time, importance, importance_score, 
                source_note, homepage_url, workflow_id,
                status, review_note
            ) VALUES (
                %s, %s, %s, 
                %s, %s, %s, %s,
                %s, %s, %s, 
                %s, %s, %s,
                %s, %s
            ) ON DUPLICATE KEY UPDATE
                title = VALUES(title),
                content = VALUES(content),
                event_tags = VALUES(event_tags),
                space_tags = VALUES(space_tags),
                impact_factors = VALUES(impact_factors),
                cat_tags = VALUES(cat_tags),
                importance_score = VALUES(importance_score),
                status = VALUES(status),
                review_note = VALUES(review_note)
            """
            
            # 从原始数据中获取publish_time, importance, source_note, homepage_url, workflow_id
            publish_time = original_data.get("publish_time", "")
            importance = original_data.get("importance", "中")  # 默认为中
            source_note = original_data.get("source_note", "")
            homepage_url = original_data.get("homepage_url", "")
            workflow_id = original_data.get("workflow_id", "")
            
            # 从审核结果中获取优化后的字段
            link_id = original_data["link_id"]
            title = review_result.get("title", original_data["title"])
            content = review_result.get("content", original_data["content"])
            event_tags = json.dumps(review_result.get("event_tags", []), ensure_ascii=False)
            space_tags = json.dumps(review_result.get("space_tags", []), ensure_ascii=False)
            impact_factors = json.dumps(review_result.get("impact_factors", []), ensure_ascii=False)
            cat_tags = json.dumps(review_result.get("cat_tags", []), ensure_ascii=False)
            importance_score = review_result.get("importance_score", 0.3)
            status = review_result.get("status", "未通过")
            review_note = review_result.get("review_note", "")
            
            # 执行插入操作
            cursor.execute(insert_query, (
                link_id, title, content, 
                event_tags, space_tags, impact_factors, cat_tags,
                publish_time, importance, importance_score, 
                source_note, homepage_url, workflow_id,
                status, review_note
            ))
            
            conn.commit()
            logger.info(f"成功将审核结果保存到true_content表，link_id: {link_id}")
            return True
            
        except mysql.connector.Error as e:
            retry_count += 1
            wait_time = 2 * retry_count  # 指数退避
            logger.warning(f"MySQL连接错误，尝试重试 ({retry_count}/{max_retries}): {str(e)}, 等待{wait_time}秒")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            time.sleep(wait_time)
        except Exception as e:
            logger.error(f"保存到true_content表失败: {str(e)}")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            return False
        finally:
            if conn:
                try:
                    cursor.close()
                    conn.close()
                except:
                    pass
    
    logger.error(f"保存到true_content表失败，已达到最大重试次数")
    return False

def check_workflow_exists(workflow_id, max_retries=3):
    """
    检查指定的workflow_id是否已经存在于true_content表中
    """
    retry_count = 0
    while retry_count < max_retries:
        conn = None
        try:
            conn = mysql.connector.connect(**MYSQL_CONFIG)
            cursor = conn.cursor()
            
            query = """
            SELECT COUNT(*) FROM news_content.true_content 
            WHERE workflow_id = %s
            LIMIT 1
            """
            
            cursor.execute(query, (workflow_id,))
            result = cursor.fetchone()
            
            # 如果计数大于0，说明workflow_id已存在
            exists = result[0] > 0
            if exists:
                logger.info(f"workflow_id: {workflow_id} 已经存在于true_content表中")
            return exists
            
        except mysql.connector.Error as e:
            retry_count += 1
            wait_time = 2 * retry_count  # 指数退避
            logger.warning(f"检查workflow_id时MySQL连接错误，尝试重试 ({retry_count}/{max_retries}): {str(e)}, 等待{wait_time}秒")
            time.sleep(wait_time)
        except Exception as e:
            logger.error(f"检查workflow_id是否存在失败: {str(e)}")
            return False
        finally:
            if conn:
                try:
                    cursor.close()
                    conn.close()
                except:
                    pass
    
    logger.error(f"检查workflow_id: {workflow_id} 失败，已达到最大重试次数")
    return False

def process_content_review(workflow_id):
    """
    处理内容审核的主函数
    """
    logger.info(f"开始处理workflow_id: {workflow_id}的内容审核")
    
    # 检查workflow_id是否已经存在
    if check_workflow_exists(workflow_id):
        logger.info(f"跳过workflow_id: {workflow_id}，已存在于数据库中")
        return True
    
    # 1. 获取去重后的link_id列表
    link_ids = get_deduplicated_link_ids(workflow_id)
    if not link_ids:
        logger.warning(f"没有找到workflow_id: {workflow_id}的去重link_id")
        return False
    
    success_count = 0
    fail_count = 0
    
    # 2. 逐个处理每个link_id
    total_count = len(link_ids)
    for index, link_id in enumerate(link_ids, 1):
        try:
            logger.info(f"处理第{index}/{total_count}条内容，link_id: {link_id}")
            
            # 3. 获取原始内容
            original_data = get_original_content(link_id)
            if not original_data:
                logger.warning(f"跳过link_id: {link_id}，未找到原始内容")
                fail_count += 1
                continue
            
            # 4. 调用阿里智能体审核内容
            review_result = call_ali_agent(original_data)
            if not review_result:
                logger.warning(f"跳过link_id: {link_id}，智能体审核失败")
                fail_count += 1
                continue
            
            # 5. 保存审核结果到true_content表
            if save_to_true_content(review_result, original_data):
                success_count += 1
            else:
                fail_count += 1
            
            # 短暂暂停，避免API请求过于频繁
            time.sleep(1)
        except Exception as e:
            logger.error(f"处理link_id: {link_id}时发生异常: {str(e)}")
            fail_count += 1
    
    logger.info(f"内容审核处理完成，成功: {success_count}，失败: {fail_count}，总计: {total_count}")
    return success_count > 0

# 从deduplication_agent.py模块中添加到监控线程处理逻辑中
def add_content_review_to_monitor():
    """
    修改app.py中的监控线程，在处理完workflow后添加内容审核步骤
    """
    logger.info("已创建content_review处理模块，请将其集成到app.py的监控线程中")
    logger.info("在处理workflow后，添加调用process_content_review(current_workflow_id)的逻辑")

if __name__ == "__main__":
    # 测试单个workflow的处理
    import sys
    if len(sys.argv) > 1:
        test_workflow_id = sys.argv[1]
        process_content_review(test_workflow_id)
    else:
        logger.info("使用方法: python process_content_review.py <workflow_id>") 