#!/usr/bin/env python
import os
import time
import json
import logging
import subprocess
import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("curl_monitor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("CurlMonitor")

# MySQL配置
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST'),
    'port': int(os.getenv('MYSQL_PORT')),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE')
}

# API服务URL (确保API服务已启动)
API_BASE_URL = "http://localhost:5001"  # 根据实际情况修改

def get_latest_workflow_info():
    """获取最新的workflow_id和最后更新时间"""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        query = """
        SELECT workflow_id, MAX(created_at) as latest_update
        FROM news_content.step3_content 
        WHERE state LIKE '%爬取成功%'
        GROUP BY workflow_id
        ORDER BY latest_update DESC 
        LIMIT 1
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result:
            return result['workflow_id'], result['latest_update']
        return None, None
    except Exception as e:
        logger.error(f"获取最新workflow_id失败: {str(e)}")
        return None, None

def execute_curl_command(workflow_id=None):
    """使用curl命令调用API进行处理"""
    # 准备curl命令
    if workflow_id:
        curl_cmd = f'curl -s -X POST {API_BASE_URL}/api/process -H "Content-Type: application/json" -d \'{{"workflow_id": "{workflow_id}"}}\''
    else:
        curl_cmd = f'curl -s -X POST {API_BASE_URL}/api/process -H "Content-Type: application/json" -d \'{{}}\''
    
    try:
        # 执行curl命令
        logger.info(f"执行curl命令: {curl_cmd}")
        result = subprocess.run(curl_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            try:
                # 解析API响应
                response = json.loads(result.stdout)
                if response.get('success'):
                    logger.info(f"成功处理workflow: {response.get('workflow_id')}")
                    return True, response.get('workflow_id')
                else:
                    logger.error(f"API调用返回失败: {response.get('message')}")
                    return False, None
            except json.JSONDecodeError:
                logger.error(f"无法解析API响应: {result.stdout}")
                return False, None
        else:
            logger.error(f"curl命令执行失败: {result.stderr}")
            return False, None
    except Exception as e:
        logger.error(f"执行curl命令时出错: {str(e)}")
        return False, None

def monitor_table_with_curl(timeout_minutes):
    """
    监控MySQL表并使用curl调用API进行处理
    
    参数:
        timeout_minutes (int): 倒计时窗口，单位分钟
    """
    logger.info(f"开始监控MySQL表，倒计时窗口设置为{timeout_minutes}分钟")
    
    last_workflow_id = None
    current_workflow_id = None
    last_update_time = None
    countdown_start = None
    
    while True:
        # 获取最新的workflow_id和时间戳
        current_workflow_id, current_update_time = get_latest_workflow_info()
        
        if not current_workflow_id:
            logger.warning("未找到任何workflow，等待30秒后重试")
            time.sleep(30)
            continue
        
        current_time = datetime.now()
        
        # 检查是否有新数据
        if current_workflow_id != last_workflow_id:
            logger.info(f"检测到新的workflow_id: {current_workflow_id}")
            last_workflow_id = current_workflow_id
            last_update_time = current_update_time
            
            # 开始或重置倒计时
            countdown_start = current_time
            countdown_end = countdown_start + timedelta(minutes=timeout_minutes)
            logger.info(f"开始倒计时，计划在 {countdown_end.strftime('%Y-%m-%d %H:%M:%S')} 处理数据")
        
        # 检查数据是否在倒计时期间更新
        if last_update_time and current_update_time and current_update_time > last_update_time:
            logger.info(f"数据已更新，重置倒计时")
            last_update_time = current_update_time
            countdown_start = current_time
            countdown_end = countdown_start + timedelta(minutes=timeout_minutes)
            logger.info(f"重置倒计时，计划在 {countdown_end.strftime('%Y-%m-%d %H:%M:%S')} 处理数据")
        
        # 如果倒计时已经开始且已经到达结束时间
        if countdown_start and current_time >= countdown_start + timedelta(minutes=timeout_minutes):
            logger.info(f"倒计时结束，开始使用curl处理workflow_id: {current_workflow_id}")
            
            # 使用curl调用API进行处理
            success, processed_id = execute_curl_command(current_workflow_id)
            
            # 重置倒计时
            countdown_start = None
            
            if success:
                logger.info(f"成功处理workflow_id: {processed_id}")
            else:
                logger.error(f"处理workflow_id: {current_workflow_id} 失败")
        
        # 如果倒计时正在进行中，显示剩余时间
        elif countdown_start:
            remaining = countdown_start + timedelta(minutes=timeout_minutes) - current_time
            remaining_minutes = int(remaining.total_seconds() / 60)
            remaining_seconds = int(remaining.total_seconds() % 60)
            logger.info(f"倒计时中: 还剩 {remaining_minutes}分{remaining_seconds}秒 后处理 workflow_id: {current_workflow_id}")
        
        # 等待一段时间后继续检查
        time.sleep(60)  # 每分钟检查一次

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='使用curl监控MySQL表数据变化并触发API处理')
    parser.add_argument('--timeout', type=int, default=10, 
                        help='倒计时窗口，单位分钟（默认为10分钟）')
    parser.add_argument('--api-url', type=str, default='http://localhost:5001',
                        help='API服务的基础URL（默认为http://localhost:5001）')
    args = parser.parse_args()
    
    # 设置API基础URL
    API_BASE_URL = args.api_url
    
    try:
        monitor_table_with_curl(args.timeout)
    except KeyboardInterrupt:
        logger.info("用户中断，监控结束")
    except Exception as e:
        logger.critical(f"监控过程出现严重错误: {str(e)}") 