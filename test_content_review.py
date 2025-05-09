#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mysql.connector
import os
import logging
import time
from dotenv import load_dotenv
from datetime import datetime
from process_content_review import process_content_review
from logging.handlers import RotatingFileHandler

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

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if logger.handlers:
    logger.handlers.clear()
logger.addHandler(log_handler)
logger.addHandler(console_handler)

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

def get_latest_workflow_id():
    """获取最新的workflow_id"""
    conn = None
    try:
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
    except Exception as e:
        logger.error(f"获取最新workflow_id失败: {str(e)}")
        if conn:
            conn.close()
        return None

def get_specific_workflow_stats(workflow_id):
    """获取指定workflow_id的统计信息"""
    conn = None
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        query = """
        SELECT 
            COUNT(*) as total_count,
            MAX(created_at) as last_update
        FROM news_content.step3_content 
        WHERE workflow_id = %s
        AND state LIKE '%爬取成功%'
        """
        
        cursor.execute(query, (workflow_id,))
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return result
    except Exception as e:
        logger.error(f"获取workflow_id {workflow_id}统计信息失败: {str(e)}")
        if conn:
            conn.close()
        return None

def main():
    """主函数"""
    # 获取最新的workflow_id
    workflow_id = get_latest_workflow_id()
    if not workflow_id:
        logger.error("无法获取最新的workflow_id")
        return
    
    # 获取workflow统计信息
    stats = get_specific_workflow_stats(workflow_id)
    if stats:
        logger.info(f"最新workflow_id: {workflow_id}")
        logger.info(f"包含 {stats['total_count']} 条记录")
        logger.info(f"最后更新时间: {stats['last_update']}")
    
    # 询问用户是否处理该workflow
    print(f"\n是否要处理workflow_id: {workflow_id}? (y/n/other)")
    choice = input().strip().lower()
    
    if choice == 'y':
        # 直接处理该workflow
        logger.info(f"开始处理workflow_id: {workflow_id}")
        start_time = time.time()
        success = process_content_review(workflow_id)
        end_time = time.time()
        
        if success:
            logger.info(f"成功处理workflow_id: {workflow_id}")
            logger.info(f"处理耗时: {end_time - start_time:.2f}秒")
        else:
            logger.error(f"处理workflow_id: {workflow_id} 失败")
    elif choice == 'n':
        # 用户输入其他workflow_id
        print("请输入要处理的workflow_id:")
        custom_workflow_id = input().strip()
        if custom_workflow_id:
            logger.info(f"开始处理自定义workflow_id: {custom_workflow_id}")
            start_time = time.time()
            success = process_content_review(custom_workflow_id)
            end_time = time.time()
            
            if success:
                logger.info(f"成功处理workflow_id: {custom_workflow_id}")
                logger.info(f"处理耗时: {end_time - start_time:.2f}秒")
            else:
                logger.error(f"处理workflow_id: {custom_workflow_id} 失败")
        else:
            logger.warning("未提供有效的workflow_id，退出处理")
    else:
        logger.info("退出处理")

if __name__ == "__main__":
    main() 