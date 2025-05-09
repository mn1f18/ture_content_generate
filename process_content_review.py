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

# 配置日志
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler = RotatingFileHandler(
    "app.log", 
    maxBytes=5*1024*1024,  # 5MB
    backupCount=2,
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
ALI_AGENT_CONTENT_EN_APP_ID = os.getenv('ALI_AGENT_CONTENT_EN_APP_ID')
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
            rows = cursor.fetchall()
            
            link_ids = [row[0] for row in rows]
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
                JSON_EXTRACT(event_tags, '$') as event_tags,
                JSON_EXTRACT(space_tags, '$') as space_tags,
                JSON_EXTRACT(impact_factors, '$') as impact_factors,
                JSON_EXTRACT(cat_tags, '$') as cat_tags,
                publish_time, importance, source_note, homepage_url, workflow_id
            FROM news_content.step3_content 
            WHERE link_id = %s
            LIMIT 1
            """
            
            cursor.execute(query, (link_id,))
            row = cursor.fetchone()
            
            if row:
                # 处理JSON字段
                result = {
                    "link_id": row["link_id"],
                    "title": row["title"],
                    "content": row["content"],
                    "publish_time": row["publish_time"],
                    "importance": row["importance"],
                    "source_note": row["source_note"],
                    "homepage_url": row["homepage_url"],
                    "workflow_id": row["workflow_id"]
                }
                
                # 解析JSON字段
                for field in ["event_tags", "space_tags", "impact_factors", "cat_tags"]:
                    try:
                        if row[field]:
                            result[field] = json.loads(row[field])
                        else:
                            result[field] = []
                    except:
                        result[field] = []
                
                logger.info(f"获取link_id: {link_id}的原始内容成功")
                return result
            else:
                logger.warning(f"未找到link_id为{link_id}的原始内容")
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

def call_ali_agent(original_content, is_english=False):
    """调用阿里智能体进行内容审核和优化
    
    Args:
        original_content: 原始内容 
        is_english: 是否调用英文版智能体
        
    Returns:
        dict: 审核结果
    """
    try:
        # 根据是否是英文选择不同的应用ID
        app_id = ALI_AGENT_CONTENT_EN_APP_ID if is_english else ALI_AGENT_CONTENT_APP_ID
        
        if not DASHSCOPE_API_KEY or not app_id:
            logger.error(f"阿里智能体API密钥或应用ID未配置，无法处理{'英文' if is_english else '中文'}内容")
            return None
        
        # 准备输入数据
        input_text = json.dumps(original_content, ensure_ascii=False)
        
        # 使用不同的API调用方式
        if use_generation_api:
            response = Generation.call(
                model=app_id,
                api_key=DASHSCOPE_API_KEY,
                prompt=input_text
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
                    app_id=app_id,
                    prompt=input_text
                )
                if response.get('code') == 'success':
                    result = response.get('output', {}).get('text')
                else:
                    logger.error(f"调用阿里智能体失败: {response.get('message', '未知错误')}")
                    return None
            except NameError:
                # 使用基础call方法
                response = call(
                    'aigc',
                    api_key=DASHSCOPE_API_KEY,
                    app_id=app_id,
                    prompt=input_text
                )
                if response.get('code') == 'success':
                    result = response.get('output', {}).get('text')
                else:
                    logger.error(f"调用阿里智能体失败: {response.get('message', '未知错误')}")
                    return None
        
        # 提取JSON结果
        try:
            if isinstance(result, str):
                # 尝试直接解析
                try:
                    return json.loads(result)
                except json.JSONDecodeError:
                    # 尝试提取JSON部分
                    logger.debug(f"尝试从结果中提取JSON: {result[:200]}...")
                    json_match = extract_json_from_text(result)
                    if json_match:
                        return json.loads(json_match)
                    else:
                        logger.error("无法从结果中提取JSON")
                        return None
            else:
                # 已经是JSON对象
                return result
        except Exception as e:
            logger.error(f"解析阿里智能体响应时出错: {str(e)}")
            return None
            
    except Exception as e:
        logger.error(f"调用阿里智能体时出错: {str(e)}")
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
                return json_str
            
        # 如果不是Markdown格式，尝试常规提取
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1:
            json_str = text[start:end+1]
            return json_str
        
        # 如果以上方法都失败，尝试直接解析整个文本
        return text
            
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
                return json_str
        except:
            pass
        return None

def save_to_true_content(review_result, original_data, is_english=False, max_retries=3):
    """
    将审核结果保存到true_content表或true_content_en表，添加重试机制
    
    参数:
    - review_result: 审核结果
    - original_data: 原始数据
    - is_english: 是否保存到英文表格
    - max_retries: 最大重试次数
    """
    table_name = "true_content_en" if is_english else "true_content"
    retry_count = 0
    
    while retry_count < max_retries:
        conn = None
        try:
            conn = mysql.connector.connect(**MYSQL_CONFIG)
            cursor = conn.cursor()
            
            # 准备插入的数据
            insert_query = f"""
            INSERT INTO news_content.{table_name} (
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
            importance = original_data.get("importance", "中" if not is_english else "Medium")  # 默认值根据语言不同
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
            status = review_result.get("status", "未通过" if not is_english else "Rejected")
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
            logger.info(f"成功将审核结果保存到{table_name}表，link_id: {link_id}")
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
            logger.error(f"保存到{table_name}表失败: {str(e)}")
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
    
    logger.error(f"保存到{table_name}表失败，已达到最大重试次数")
    return False

def check_workflow_exists(workflow_id, is_english=False, max_retries=3):
    """
    检查指定的workflow_id是否已经存在于true_content表或true_content_en表中
    
    参数:
    - workflow_id: 工作流ID
    - is_english: 是否检查英文表格
    - max_retries: 最大重试次数
    """
    table_name = "true_content_en" if is_english else "true_content"
    retry_count = 0
    
    while retry_count < max_retries:
        conn = None
        try:
            conn = mysql.connector.connect(**MYSQL_CONFIG)
            cursor = conn.cursor()
            
            query = f"""
            SELECT COUNT(*) FROM news_content.{table_name} 
            WHERE workflow_id = %s
            LIMIT 1
            """
            
            cursor.execute(query, (workflow_id,))
            result = cursor.fetchone()
            
            # 如果计数大于0，说明workflow_id已存在
            exists = result[0] > 0
            if exists:
                logger.info(f"workflow_id: {workflow_id} 已经存在于{table_name}表中")
            return exists
            
        except mysql.connector.Error as e:
            retry_count += 1
            wait_time = 2 * retry_count  # 指数退避
            logger.warning(f"检查workflow_id时MySQL连接错误，尝试重试 ({retry_count}/{max_retries}): {str(e)}, 等待{wait_time}秒")
            time.sleep(wait_time)
        except Exception as e:
            logger.error(f"检查workflow_id是否存在于{table_name}表中失败: {str(e)}")
            return False
        finally:
            if conn:
                try:
                    cursor.close()
                    conn.close()
                except:
                    pass
    
    logger.error(f"检查workflow_id: {workflow_id} 在{table_name}表中是否存在失败，已达到最大重试次数")
    return False

def process_content_review(workflow_id):
    """
    处理内容审核的主函数
    """
    logger.info(f"开始处理workflow_id: {workflow_id}的内容审核")
    
    # 检查中文表和英文表中是否已存在该workflow_id
    cn_exists = check_workflow_exists(workflow_id, is_english=False)
    en_exists = check_workflow_exists(workflow_id, is_english=True)
    
    # 如果中文和英文表都已存在，则跳过处理
    if cn_exists and en_exists:
        logger.info(f"跳过workflow_id: {workflow_id}，已同时存在于中英文数据库中")
        return True
    
    # 1. 获取去重后的link_id列表
    link_ids = get_deduplicated_link_ids(workflow_id)
    if not link_ids:
        logger.warning(f"没有找到workflow_id: {workflow_id}的去重link_id")
        return False
    
    # 统计成功和失败数量
    cn_success_count = 0
    cn_fail_count = 0
    en_success_count = 0
    en_fail_count = 0
    
    # 2. 逐个处理每个link_id
    total_count = len(link_ids)
    for index, link_id in enumerate(link_ids, 1):
        try:
            logger.info(f"处理第{index}/{total_count}条内容，link_id: {link_id}")
            
            # 3. 获取原始内容
            original_data = get_original_content(link_id)
            if not original_data:
                logger.warning(f"跳过link_id: {link_id}，未找到原始内容")
                cn_fail_count += 1
                en_fail_count += 1
                continue
            
            # 4a. 如果中文表不存在该workflow_id，调用中文智能体审核内容
            if not cn_exists:
                cn_review_result = call_ali_agent(original_data, is_english=False)
                if not cn_review_result:
                    logger.warning(f"跳过link_id: {link_id}，中文智能体审核失败")
                    cn_fail_count += 1
                else:
                    # 5a. 保存中文审核结果到true_content表
                    if save_to_true_content(cn_review_result, original_data, is_english=False):
                        cn_success_count += 1
                    else:
                        cn_fail_count += 1
            
            # 4b. 如果英文表不存在该workflow_id，调用英文智能体审核内容
            if not en_exists:
                # 短暂暂停，避免API请求过于频繁
                time.sleep(1)
                
                en_review_result = call_ali_agent(original_data, is_english=True)
                if not en_review_result:
                    logger.warning(f"跳过link_id: {link_id}，英文智能体审核失败")
                    en_fail_count += 1
                else:
                    # 5b. 保存英文审核结果到true_content_en表
                    if save_to_true_content(en_review_result, original_data, is_english=True):
                        en_success_count += 1
                    else:
                        en_fail_count += 1
            
            # 短暂暂停，避免API请求过于频繁
            time.sleep(1)
        except Exception as e:
            logger.error(f"处理link_id: {link_id}时发生异常: {str(e)}")
            cn_fail_count += 1 if not cn_exists else 0
            en_fail_count += 1 if not en_exists else 0
    
    # 记录处理结果
    if not cn_exists:
        logger.info(f"中文内容审核处理完成，成功: {cn_success_count}，失败: {cn_fail_count}，总计: {total_count}")
    if not en_exists:
        logger.info(f"英文内容审核处理完成，成功: {en_success_count}，失败: {en_fail_count}，总计: {total_count}")
    
    # 只要有一种语言处理成功，就返回成功
    return (cn_success_count > 0 or not cn_exists) or (en_success_count > 0 or not en_exists)

# 从deduplication_agent.py模块中添加到监控线程处理逻辑中
def add_content_review_to_monitor():
    """
    修改app.py中的监控线程，在处理完workflow后添加内容审核步骤
    """
    logger.info("已创建content_review处理模块，请将其集成到app.py的监控线程中")
    logger.info("在处理workflow后，添加调用process_content_review(current_workflow_id)的逻辑")

def process_single_article(article_data):
    """处理单篇文章，调用智能体进行内容优化"""
    if not article_data:
        logger.error("文章数据为空，无法处理")
        return False
    
    link_id = article_data.get('link_id')
    if not link_id:
        logger.error("文章缺少link_id，无法处理")
        return False
    
    logger.info(f"开始处理文章 link_id: {link_id}")
    
    try:
        # 准备输入数据
        input_data = {
            "article": {
                "link_id": article_data["link_id"],
                "title": article_data["title"],
                "content": article_data["content"],
                "event_tags": article_data["event_tags"],
                "space_tags": article_data["space_tags"],
                "cat_tags": article_data["cat_tags"],
                "impact_factors": article_data["impact_factors"]
            }
        }
        
        # 调用中文内容审核
        logger.info(f"调用中文内容审核智能体，link_id: {link_id}")
        zh_result = call_ali_agent(input_data, is_english=False)
        
        if not zh_result or not isinstance(zh_result, dict):
            logger.error(f"中文内容审核失败或结果格式错误，link_id: {link_id}")
            return False
        
        # 调用英文内容审核
        logger.info(f"调用英文内容审核智能体，link_id: {link_id}")
        en_result = call_ali_agent(input_data, is_english=True)
        
        if not en_result or not isinstance(en_result, dict):
            logger.error(f"英文内容审核失败或结果格式错误，link_id: {link_id}")
            return False
        
        # 保存结果到数据库
        success = save_to_true_content(zh_result, en_result, article_data['workflow_id'])
        if success:
            logger.info(f"成功处理并保存文章 link_id: {link_id}")
            return True
        else:
            logger.error(f"保存结果到数据库失败，link_id: {link_id}")
            return False
            
    except Exception as e:
        logger.error(f"处理文章出错 link_id: {link_id}, 错误: {str(e)}")
        return False

if __name__ == "__main__":
    # 测试单个workflow的处理
    import sys
    if len(sys.argv) > 1:
        test_workflow_id = sys.argv[1]
        process_content_review(test_workflow_id)
    else:
        logger.info("使用方法: python process_content_review.py <workflow_id>") 