import os
import time
import logging
import mysql.connector
from datetime import datetime, timedelta
import subprocess
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("timer_monitor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TimerMonitor")

# MySQL配置
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST'),
    'port': int(os.getenv('MYSQL_PORT')),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE')
}

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

def run_deduplication_job(workflow_id):
    """执行去重任务"""
    cmd = ['python', 'deduplication_agent.py', '--workflow_id', workflow_id]
    
    try:
        logger.info(f"开始处理workflow_id: {workflow_id}")
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        output = result.stdout.strip()
        logger.info(f"去重任务执行完成: {output}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"去重任务执行失败: {e}, 错误输出: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"执行过程中出现异常: {str(e)}")
        return False

def monitor_with_timeout(timeout_minutes):
    """
    监控新数据并使用倒计时窗口
    
    参数:
        timeout_minutes (int): 倒计时窗口，单位分钟
    """
    logger.info(f"开始监控，倒计时窗口设置为{timeout_minutes}分钟")
    
    last_workflow_id = None
    current_workflow_id = None
    countdown_start = None
    
    while True:
        # 获取最新的workflow_id和时间戳
        current_workflow_id, update_time = get_latest_workflow_info()
        
        if not current_workflow_id:
            logger.warning("未找到任何workflow，等待10秒后重试")
            time.sleep(10)
            continue
        
        current_time = datetime.now()
        
        # 检查是否有新数据
        if current_workflow_id != last_workflow_id:
            logger.info(f"检测到新的workflow_id: {current_workflow_id}")
            last_workflow_id = current_workflow_id
            
            # 开始或重置倒计时
            countdown_start = current_time
            countdown_end = countdown_start + timedelta(minutes=timeout_minutes)
            logger.info(f"开始倒计时，计划在 {countdown_end.strftime('%Y-%m-%d %H:%M:%S')} 处理数据")
        
        # 如果倒计时已经开始且已经到达结束时间
        if countdown_start and current_time >= countdown_start + timedelta(minutes=timeout_minutes):
            logger.info(f"倒计时结束，开始处理workflow_id: {current_workflow_id}")
            
            # 执行去重处理
            success = run_deduplication_job(current_workflow_id)
            
            # 重置倒计时和最后处理的workflow_id
            countdown_start = None
            
            if success:
                logger.info(f"成功处理workflow_id: {current_workflow_id}")
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
    
    parser = argparse.ArgumentParser(description='监控新闻数据并在倒计时结束后自动处理')
    parser.add_argument('--timeout', type=int, default=10, 
                        help='倒计时窗口，单位分钟（默认为10分钟）')
    args = parser.parse_args()
    
    try:
        monitor_with_timeout(args.timeout)
    except KeyboardInterrupt:
        logger.info("用户中断，监控结束")
    except Exception as e:
        logger.critical(f"监控过程出现严重错误: {str(e)}") 