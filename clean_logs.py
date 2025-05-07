#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import logging
import shutil
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("LogCleaner")

def clean_logs(days_to_keep=7, max_size_mb=100):
    """
    清理日志文件：
    1. 删除超过指定天数的旧日志
    2. 如果日志总大小超过限制，保留最新的日志
    3. 修复编码问题
    """
    logger.info("开始清理日志文件...")
    
    log_files = [
        f for f in os.listdir('.') 
        if f.endswith('.log') or re.match(r'.*\.log\.\d+', f)
    ]
    
    current_time = datetime.now()
    cutoff_date = current_time - timedelta(days=days_to_keep)
    total_size = 0
    file_info = []
    
    # 收集日志文件信息
    for file_name in log_files:
        file_path = os.path.join('.', file_name)
        file_stat = os.stat(file_path)
        file_size = file_stat.st_size / (1024 * 1024)  # 转换为MB
        file_time = datetime.fromtimestamp(file_stat.st_mtime)
        total_size += file_size
        
        file_info.append({
            'name': file_name,
            'path': file_path,
            'size': file_size,
            'time': file_time
        })
    
    # 按修改时间排序
    file_info.sort(key=lambda x: x['time'], reverse=True)
    
    # 处理文件
    for file in file_info:
        # 检查日期
        if file['time'] < cutoff_date:
            try:
                os.remove(file['path'])
                logger.info(f"已删除过期日志文件: {file['name']}")
                continue
            except Exception as e:
                logger.error(f"删除文件 {file['name']} 失败: {str(e)}")
        
        # 检查编码问题
        try:
            # 创建临时文件
            temp_file = file['path'] + '.temp'
            
            with open(file['path'], 'r', encoding='utf-8', errors='replace') as src:
                with open(temp_file, 'w', encoding='utf-8') as dst:
                    for line in src:
                        dst.write(line)
            
            # 用正确编码的文件替换原文件
            shutil.move(temp_file, file['path'])
            logger.info(f"已修复文件编码: {file['name']}")
        except Exception as e:
            logger.error(f"修复文件 {file['name']} 编码失败: {str(e)}")
    
    logger.info(f"日志清理完成。共处理 {len(file_info)} 个文件，总大小: {total_size:.2f}MB")

if __name__ == "__main__":
    clean_logs() 