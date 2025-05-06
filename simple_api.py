import os
import json
import logging
import mysql.connector
import psycopg2
from datetime import datetime
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
        logging.FileHandler("simple_api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("SimpleAPI")

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

@app.route('/api/status', methods=['GET'])
def api_status():
    """API状态检查"""
    return jsonify({
        'status': 'online',
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

@app.route('/api/workflows/latest', methods=['GET'])
def get_latest_workflow():
    """获取最新的workflow信息"""
    try:
        workflow_id = get_latest_workflow_id()
        if workflow_id:
            # 获取该workflow的新闻数量
            news_data = get_news_by_workflow(workflow_id)
            count = len(news_data) if news_data else 0
            
            return jsonify({
                'success': True,
                'workflow_id': workflow_id,
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
        # 直接调用处理函数（无需启动新进程）
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
            }
        ]
    }
    
    return jsonify(docs)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001) 