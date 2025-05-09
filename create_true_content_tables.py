#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import mysql.connector
import logging
from dotenv import load_dotenv
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

logger = logging.getLogger("DBSetup")
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

# 创建表的SQL语句
CREATE_TRUE_CONTENT_TABLE = """
CREATE TABLE IF NOT EXISTS news_content.true_content (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    link_id VARCHAR(50) NOT NULL UNIQUE,      -- 唯一标识符
    title VARCHAR(255) NOT NULL,              -- 优化后的标题
    content TEXT NOT NULL,                    -- 优化后的内容
    event_tags JSON NOT NULL,                 -- 事件标签
    space_tags JSON NOT NULL,                 -- 国家标签
    impact_factors JSON NOT NULL,             -- 影响因素
    cat_tags JSON NOT NULL,                   -- 品类标签
    publish_time VARCHAR(10),                 -- 发布时间
    importance VARCHAR(10) NOT NULL,          -- 重要程度：高、中、低
    importance_score FLOAT NOT NULL,          -- 重要程度分数(0-1)，用于前端排序
    source_note TEXT,                         -- 来源备注
    homepage_url VARCHAR(255),                -- 原文链接
    workflow_id VARCHAR(50) NOT NULL,         -- 工作流ID
    status VARCHAR(20) NOT NULL,              -- 状态：可上架、未通过
    review_note TEXT,                         -- 阿里agent审核评价
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 创建时间
    INDEX idx_link_id (link_id),
    INDEX idx_workflow_id (workflow_id),
    INDEX idx_importance_score (importance_score),
    INDEX idx_status (status)
);
"""

CREATE_TRUE_CONTENT_EN_TABLE = """
CREATE TABLE IF NOT EXISTS news_content.true_content_en (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    link_id VARCHAR(50) NOT NULL UNIQUE,      -- 对应中文版的link_id
    title VARCHAR(255) NOT NULL,              -- 英文标题
    content TEXT NOT NULL,                    -- 英文内容
    event_tags JSON NOT NULL,                 -- 英文事件标签
    space_tags JSON NOT NULL,                 -- 国家标签(英文)
    impact_factors JSON NOT NULL,             -- 影响因素(英文)
    cat_tags JSON NOT NULL,                   -- 品类标签(英文)
    publish_time VARCHAR(10),                 -- 发布时间
    importance VARCHAR(10) NOT NULL,          -- 重要程度(英文表述)
    importance_score FLOAT NOT NULL,          -- 重要程度分数(0-1)
    source_note TEXT,                         -- 来源备注(英文)
    homepage_url VARCHAR(255),                -- 原文链接
    workflow_id VARCHAR(50) NOT NULL,         -- 工作流ID
    status VARCHAR(20) NOT NULL,              -- 状态：Approved、Rejected
    review_note TEXT,                         -- 阿里agent审核评价(英文)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 创建时间
    INDEX idx_link_id (link_id),
    INDEX idx_workflow_id (workflow_id),
    INDEX idx_importance_score (importance_score),
    INDEX idx_status (status)
);
"""

def create_tables():
    """创建MySQL表格"""
    conn = None
    try:
        # 连接到MySQL
        logger.info("正在连接到MySQL...")
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        # 创建true_content表
        logger.info("开始创建true_content表...")
        cursor.execute(CREATE_TRUE_CONTENT_TABLE)
        logger.info("true_content表创建成功")
        
        # 创建true_content_en表
        logger.info("开始创建true_content_en表...")
        cursor.execute(CREATE_TRUE_CONTENT_EN_TABLE)
        logger.info("true_content_en表创建成功")
        
        # 提交事务
        conn.commit()
        logger.info("表格创建完成！")
        
    except Exception as e:
        logger.error(f"创建表格失败: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()
            logger.info("MySQL连接已关闭")

if __name__ == "__main__":
    create_tables() 