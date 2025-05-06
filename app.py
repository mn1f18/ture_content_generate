import os
import json
import logging
import subprocess
import mysql.connector
import psycopg2
from datetime import datetime
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DeduplicationAPI")

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

def get_latest_workflow_id():
    """获取最新的workflow_id"""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        query = """
        SELECT workflow_id, MAX(created_at) as latest_update
        FROM step3_content 
        WHERE state = '{"状态": "爬取成功"}'
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

def run_deduplication_job(workflow_id=None):
    """执行去重任务，可选指定workflow_id"""
    cmd = ['python', 'deduplication_agent.py']
    if workflow_id:
        cmd.extend(['--workflow_id', workflow_id])
        
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        output = result.stdout.strip()
        logger.info(f"去重任务执行完成: {output}")
        return True, output
    except subprocess.CalledProcessError as e:
        error_msg = f"去重任务执行失败: {e}, 错误输出: {e.stderr}"
        logger.error(error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"执行过程中出现异常: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

@app.route('/api/process', methods=['POST'])
def process_now():
    """立即处理workflow"""
    data = request.get_json() or {}
    workflow_id = data.get('workflow_id')
    
    # 如果未指定workflow_id，获取最新的
    if not workflow_id:
        workflow_id, _ = get_latest_workflow_id()
        if not workflow_id:
            return jsonify({
                'success': False,
                'message': '没有找到可处理的workflow_id'
            })
    
    logger.info(f"开始处理workflow_id: {workflow_id}")
    success, message = run_deduplication_job(workflow_id)
    
    return jsonify({
        'success': success,
        'message': message,
        'workflow_id': workflow_id
    })

@app.route('/api/workflows', methods=['GET'])
def get_workflows():
    """获取最近的workflow_id列表"""
    try:
        limit = int(request.args.get('limit', 10))
        
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        query = """
        SELECT DISTINCT workflow_id, MAX(created_at) as latest_update
        FROM step3_content 
        WHERE state = '{"状态": "爬取成功"}'
        GROUP BY workflow_id
        ORDER BY latest_update DESC 
        LIMIT %s
        """
        
        cursor.execute(query, (limit,))
        workflows = [{'workflow_id': row['workflow_id'], 'latest_update': str(row['latest_update'])} 
                    for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'workflows': workflows
        })
    except Exception as e:
        logger.error(f"获取workflow列表失败: {str(e)}")
        return jsonify({
            'success': False,
            'message': f"获取workflow列表失败: {str(e)}"
        })

@app.route('/', methods=['GET'])
def home():
    """主页，显示简单的使用说明"""
    docs = {
        'api_endpoints': [
            {
                'url': '/api/process',
                'method': 'POST',
                'description': '处理workflow，可选参数: {"workflow_id": "your_workflow_id"}',
                'curl_example': 'curl -X POST http://localhost:5001/api/process -H "Content-Type: application/json" -d \'{"workflow_id": "your_workflow_id"}\''
            },
            {
                'url': '/api/workflows',
                'method': 'GET',
                'description': '获取最近的workflow列表，可选参数: ?limit=10',
                'curl_example': 'curl http://localhost:5001/api/workflows?limit=5'
            }
        ]
    }
    
    return jsonify(docs)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001) 