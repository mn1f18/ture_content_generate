import os
import json
import logging
import mysql.connector
import psycopg2
import time
import threading
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# 导入去重代理模块的核心函数
from deduplication_agent import process_workflow, get_latest_workflow_id, get_news_by_workflow

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("API")

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

app = Flask(__name__)

# 全局变量，存储监控状态
monitor_state = {
    'is_monitoring': False,
    'last_workflow_id': None,
    'countdown_start': None,
    'countdown_minutes': 10,  # 默认倒计时10分钟
    'monitor_thread': None
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

def get_news_count_by_workflow(workflow_id):
    """获取指定workflow_id的新闻条目数量"""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        query = """
        SELECT COUNT(*) as count
        FROM news_content.step3_content 
        WHERE workflow_id = %s
        AND state LIKE '%爬取成功%'
        AND importance IN ('高', '中')
        """
        
        cursor.execute(query, (workflow_id,))
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result:
            return result[0]
        return 0
    except Exception as e:
        logger.error(f"获取workflow_id {workflow_id}的新闻数量失败: {str(e)}")
        return 0

def monitoring_thread():
    """监控线程，检查数据更新并倒计时处理"""
    global monitor_state
    
    last_workflow_id = monitor_state['last_workflow_id']
    last_news_count = 0
    countdown_start = None
    no_update_timer = None
    
    logger.info(f"开始监控，倒计时窗口设置为{monitor_state['countdown_minutes']}分钟")
    
    while monitor_state['is_monitoring']:
        # 获取最新的workflow_id和时间戳
        current_workflow_id, update_time = get_latest_workflow_info()
        
        if not current_workflow_id:
            logger.warning("未找到任何workflow，等待30秒后重试")
            time.sleep(30)
            continue
        
        current_time = datetime.now()
        
        # 如果是新的workflow_id
        if current_workflow_id != last_workflow_id:
            logger.info(f"检测到新的workflow_id: {current_workflow_id}")
            last_workflow_id = current_workflow_id
            monitor_state['last_workflow_id'] = current_workflow_id
            last_news_count = get_news_count_by_workflow(current_workflow_id)
            logger.info(f"初始新闻数量: {last_news_count}")
            
            # 重置倒计时和无更新计时器
            countdown_start = None
            monitor_state['countdown_start'] = None
            no_update_timer = current_time
        else:
            # 检查当前workflow_id的新闻数量是否有变化
            current_news_count = get_news_count_by_workflow(current_workflow_id)
            
            if current_news_count > last_news_count:
                # 有新的新闻条目
                logger.info(f"检测到新的新闻条目: {current_news_count - last_news_count}条 (总计: {current_news_count}条)")
                last_news_count = current_news_count
                
                # 重置无更新计时器
                no_update_timer = current_time
                
                # 如果倒计时未开始，则不做任何操作
                if countdown_start is None:
                    logger.info(f"继续收集数据中...")
            elif no_update_timer is not None:
                # 如果10分钟内没有新的新闻条目，开始倒计时
                if countdown_start is None and (current_time - no_update_timer).total_seconds() >= 600:  # 10分钟
                    logger.info(f"10分钟内没有新的新闻条目，开始倒计时...")
                    countdown_start = current_time
                    monitor_state['countdown_start'] = countdown_start
                    countdown_end = countdown_start + timedelta(minutes=monitor_state['countdown_minutes'])
                    logger.info(f"开始倒计时，计划在 {countdown_end.strftime('%Y-%m-%d %H:%M:%S')} 处理数据")
        
        # 如果倒计时已经开始且已经到达结束时间
        if countdown_start and current_time >= countdown_start + timedelta(minutes=monitor_state['countdown_minutes']):
            logger.info(f"倒计时结束，开始处理workflow_id: {current_workflow_id}, 共{last_news_count}条新闻")
            
            # 处理workflow
            try:
                success = process_workflow(current_workflow_id)
                if success:
                    logger.info(f"成功处理workflow_id: {current_workflow_id}")
                else:
                    logger.error(f"处理workflow_id: {current_workflow_id} 失败")
            except Exception as e:
                logger.error(f"处理workflow时出错: {str(e)}")
            
            # 重置倒计时
            countdown_start = None
            monitor_state['countdown_start'] = None
            
            # 重置workflow_id，强制检查是否有新的workflow_id
            last_workflow_id = None
            monitor_state['last_workflow_id'] = None
            logger.info(f"重置workflow_id监控状态，准备检查新的workflow")
        
        # 如果倒计时正在进行中，显示剩余时间
        elif countdown_start:
            remaining = countdown_start + timedelta(minutes=monitor_state['countdown_minutes']) - current_time
            remaining_minutes = int(remaining.total_seconds() / 60)
            remaining_seconds = int(remaining.total_seconds() % 60)
            logger.info(f"倒计时中: 还剩 {remaining_minutes}分{remaining_seconds}秒 后处理 workflow_id: {current_workflow_id}")
        # 没有倒计时但监控仍在进行
        else:
            # 计算自上次更新以来的时间
            if no_update_timer:
                minutes_since_update = int((current_time - no_update_timer).total_seconds() / 60)
                logger.info(f"持续监控中: workflow_id: {current_workflow_id}, 当前新闻数: {last_news_count}, 已有{minutes_since_update}分钟无新增")
            else:
                logger.info(f"持续监控中: workflow_id: {current_workflow_id}, 当前新闻数: {last_news_count}")
        
        # 等待一段时间后继续检查
        time.sleep(60)  # 每分钟检查一次

@app.route('/api/status', methods=['GET'])
def api_status():
    """API状态检查"""
    return jsonify({
        'status': 'online',
        'is_monitoring': monitor_state['is_monitoring'],
        'last_workflow_id': monitor_state['last_workflow_id'],
        'countdown_start': monitor_state['countdown_start'].strftime('%Y-%m-%d %H:%M:%S') if monitor_state['countdown_start'] else None,
        'countdown_minutes': monitor_state['countdown_minutes'],
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

@app.route('/api/workflows/latest', methods=['GET'])
def get_latest_workflow():
    """获取最新的workflow信息"""
    try:
        workflow_id, update_time = get_latest_workflow_info()
        if workflow_id:
            # 获取该workflow的新闻数量
            news_data = get_news_by_workflow(workflow_id)
            count = len(news_data) if news_data else 0
            
            return jsonify({
                'success': True,
                'workflow_id': workflow_id,
                'update_time': update_time.strftime('%Y-%m-%d %H:%M:%S') if update_time else None,
                'news_count': count
            })
        else:
            return jsonify({
                'success': False,
                'message': '没有找到最新的workflow'
            })
    except Exception as e:
        logger.error(f"获取最新workflow失败: {str(e)}")
        return jsonify({
            'success': False,
            'message': f"获取最新workflow失败: {str(e)}"
        }), 500

@app.route('/api/process/latest', methods=['POST'])
def process_latest():
    """处理最新的workflow"""
    try:
        # 直接调用处理函数
        workflow_id = get_latest_workflow_id()
        if not workflow_id:
            return jsonify({
                'success': False,
                'message': '没有找到最新的workflow'
            }), 404
            
        logger.info(f"开始处理最新workflow: {workflow_id}")
        success = process_workflow(workflow_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': f"成功处理workflow: {workflow_id}"
            })
        else:
            return jsonify({
                'success': False,
                'message': f"处理workflow失败: {workflow_id}"
            }), 500
    except Exception as e:
        logger.error(f"处理最新workflow时出错: {str(e)}")
        return jsonify({
            'success': False,
            'message': f"处理最新workflow时出错: {str(e)}"
        }), 500

@app.route('/api/process/<workflow_id>', methods=['POST'])
def process_specific(workflow_id):
    """处理指定的workflow"""
    try:
        logger.info(f"开始处理指定workflow: {workflow_id}")
        success = process_workflow(workflow_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': f"成功处理workflow: {workflow_id}"
            })
        else:
            return jsonify({
                'success': False,
                'message': f"处理workflow失败: {workflow_id}"
            }), 500
    except Exception as e:
        logger.error(f"处理指定workflow时出错: {str(e)}")
        return jsonify({
            'success': False,
            'message': f"处理指定workflow时出错: {str(e)}"
        }), 500

@app.route('/api/monitor/start', methods=['POST'])
def start_monitoring():
    """开始监控模式"""
    global monitor_state
    
    try:
        data = request.get_json(silent=True) or {}
        
        # 如果已经在监控中，返回错误
        if monitor_state['is_monitoring']:
            return jsonify({
                'success': False,
                'message': '监控已经在运行中'
            })
        
        # 设置倒计时时间（可选）
        if 'minutes' in data:
            try:
                minutes = int(data['minutes'])
                if minutes > 0:
                    monitor_state['countdown_minutes'] = minutes
            except (ValueError, TypeError):
                pass
        
        # 启动监控线程
        monitor_state['is_monitoring'] = True
        monitor_state['monitor_thread'] = threading.Thread(target=monitoring_thread)
        monitor_state['monitor_thread'].daemon = True
        monitor_state['monitor_thread'].start()
        
        return jsonify({
            'success': True,
            'message': f"监控已启动，倒计时设置为{monitor_state['countdown_minutes']}分钟",
            'countdown_minutes': monitor_state['countdown_minutes']
        })
    except Exception as e:
        logger.error(f"启动监控时出错: {str(e)}")
        return jsonify({
            'success': False,
            'message': f"启动监控时出错: {str(e)}"
        }), 500

@app.route('/api/monitor/stop', methods=['POST'])
def stop_monitoring():
    """停止监控模式"""
    global monitor_state
    
    try:
        if not monitor_state['is_monitoring']:
            return jsonify({
                'success': False,
                'message': '监控未在运行'
            })
        
        # 停止监控线程
        monitor_state['is_monitoring'] = False
        
        # 等待线程结束
        if monitor_state['monitor_thread']:
            monitor_state['monitor_thread'].join(timeout=2.0)
        
        monitor_state['monitor_thread'] = None
        monitor_state['countdown_start'] = None
        
        return jsonify({
            'success': True,
            'message': '监控已停止'
        })
    except Exception as e:
        logger.error(f"停止监控时出错: {str(e)}")
        return jsonify({
            'success': False,
            'message': f"停止监控时出错: {str(e)}"
        }), 500

@app.route('/', methods=['GET'])
def home():
    """主页，显示简单的使用说明"""
    docs = {
        'api_endpoints': [
            {
                'url': '/api/status',
                'method': 'GET',
                'description': '检查API服务状态',
                'curl_example': 'curl http://localhost:5001/api/status'
            },
            {
                'url': '/api/workflows/latest',
                'method': 'GET',
                'description': '获取最新的workflow信息',
                'curl_example': 'curl http://localhost:5001/api/workflows/latest'
            },
            {
                'url': '/api/process/latest',
                'method': 'POST',
                'description': '处理最新的workflow',
                'curl_example': 'curl -X POST http://localhost:5001/api/process/latest'
            },
            {
                'url': '/api/process/<workflow_id>',
                'method': 'POST',
                'description': '处理指定的workflow',
                'curl_example': 'curl -X POST http://localhost:5001/api/process/your_workflow_id'
            },
            {
                'url': '/api/monitor/start',
                'method': 'POST',
                'description': '开始监控模式，可选参数: {"minutes": 10}',
                'curl_example': 'curl -X POST http://localhost:5001/api/monitor/start -H "Content-Type: application/json" -d \'{"minutes": 10}\''
            },
            {
                'url': '/api/monitor/stop',
                'method': 'POST',
                'description': '停止监控模式',
                'curl_example': 'curl -X POST http://localhost:5001/api/monitor/stop'
            }
        ]
    }
    
    return jsonify(docs)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001) 