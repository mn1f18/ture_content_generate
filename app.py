import os
import json
import logging
import mysql.connector
import mysql.connector.pooling
import psycopg2
import psycopg2.pool
import time
import threading
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from logging.handlers import RotatingFileHandler

# 导入去重代理模块的核心函数
from deduplication_agent import process_workflow, get_latest_workflow_id, get_news_by_workflow
# 导入内容审核处理模块
from process_content_review import process_content_review

# 加载环境变量
load_dotenv()

# 配置日志
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler = RotatingFileHandler(
    "logs/app.log", 
    maxBytes=5*1024*1024,  # 5MB
    backupCount=3,
    encoding='utf-8'
)
log_handler.setFormatter(log_formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

# 确保删除已存在的处理器，避免重复添加
logger = logging.getLogger("API")
logger.setLevel(logging.INFO)
if logger.handlers:
    logger.handlers.clear()
logger.addHandler(log_handler)
logger.addHandler(console_handler)
logger.propagate = False  # 防止日志传播到根日志器

# 设置第三方库日志级别为WARNING，减少噪音
logging.getLogger("mysql.connector").setLevel(logging.WARNING)
logging.getLogger("werkzeug").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

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

# 创建数据库连接池，添加重试机制
def initialize_connection_pools(max_retries=3):
    """初始化数据库连接池，带重试机制"""
    global mysql_pool, pg_pool
    
    # MySQL连接池初始化
    mysql_retry_count = 0
    mysql_success = False
    
    while mysql_retry_count < max_retries and not mysql_success:
        try:
            # MySQL连接池配置
            mysql_pool_config = MYSQL_CONFIG.copy()
            mysql_pool_config.update({
                'pool_name': 'mysql_pool',
                'pool_size': 5,  # 连接池大小
                'pool_reset_session': True,  # 重置会话状态
                'connect_timeout': 30,  # 连接超时时间
            })
            
            # 创建MySQL连接池
            globals()['mysql_pool'] = mysql.connector.pooling.MySQLConnectionPool(**mysql_pool_config)
            logger.info("MySQL连接池初始化成功")
            mysql_success = True
            
        except Exception as e:
            mysql_retry_count += 1
            wait_time = 2 ** mysql_retry_count  # 指数退避
            
            if mysql_retry_count < max_retries:
                logger.warning(f"MySQL连接池初始化失败，尝试第{mysql_retry_count+1}次重试: {str(e)}，等待{wait_time}秒")
                time.sleep(wait_time)
            else:
                logger.error(f"MySQL连接池初始化最终失败: {str(e)}，将使用非连接池方式")
    
    # PostgreSQL连接池初始化
    pg_retry_count = 0
    pg_success = False
    
    while pg_retry_count < max_retries and not pg_success:
        try:
            # PostgreSQL连接池配置
            pg_pool_config = PG_CONFIG.copy()
            # 创建PostgreSQL连接池
            globals()['pg_pool'] = psycopg2.pool.ThreadedConnectionPool(
                minconn=3,  # 增加最小连接数
                maxconn=20,  # 增加最大连接数
                user=pg_pool_config['user'],
                password=pg_pool_config['password'],
                host=pg_pool_config['host'],
                port=pg_pool_config['port'],
                database=pg_pool_config['dbname']
            )
            logger.info("PostgreSQL连接池初始化成功")
            pg_success = True
            
        except Exception as e:
            pg_retry_count += 1
            wait_time = 2 ** pg_retry_count  # 指数退避
            
            if pg_retry_count < max_retries:
                logger.warning(f"PostgreSQL连接池初始化失败，尝试第{pg_retry_count+1}次重试: {str(e)}，等待{wait_time}秒")
                time.sleep(wait_time)
            else:
                logger.error(f"PostgreSQL连接池初始化最终失败: {str(e)}，将使用非连接池方式")
    
    # 初始化跟踪集合
    if 'pg_pool' in globals() and '_pg_pooled_connections' not in globals():
        globals()['_pg_pooled_connections'] = set()
    
    return mysql_success or pg_success

# 执行初始化
initialize_connection_pools(max_retries=3)

# 健康检查定时器
last_health_check = datetime.now()
db_health_status = {
    'mysql': True,
    'postgres': True
}

def get_pool_stats():
    """获取数据库连接池状态统计"""
    stats = {
        'mysql': {
            'pool_exists': 'mysql_pool' in globals(),
            'active_connections': 0
        },
        'postgres': {
            'pool_exists': 'pg_pool' in globals(),
            'active_connections': 0
        }
    }
    
    # 获取MySQL连接池状态
    if stats['mysql']['pool_exists']:
        try:
            # 注意：这些属性依赖于mysql.connector的实现，可能需要调整
            if hasattr(mysql_pool, '_cnx_queue'):
                stats['mysql']['active_connections'] = mysql_pool._cnx_queue.qsize()
        except:
            pass
    
    # 获取PostgreSQL连接池状态
    if stats['postgres']['pool_exists']:
        try:
            stats['postgres']['active_connections'] = len(pg_pool._used)
        except:
            pass
    
    return stats

def check_db_connection_health():
    """定期检查数据库连接健康状态，并输出连接池统计信息"""
    global last_health_check, db_health_status
    
    # 每10分钟检查一次
    current_time = datetime.now()
    if (current_time - last_health_check).total_seconds() < 600:  # 10分钟
        return db_health_status
    
    last_health_check = current_time
    logger.info("执行数据库连接健康检查...")
    
    # 获取连接池统计信息
    pool_stats = get_pool_stats()
    logger.info(f"连接池状态: MySQL({pool_stats['mysql']['active_connections']}), PostgreSQL({pool_stats['postgres']['active_connections']})")
    
    # 检查是否需要重置PostgreSQL连接池
    if 'pg_pool' in globals() and pool_stats['postgres']['active_connections'] >= 12:  # 将阈值从3调整为12
        logger.warning("PostgreSQL连接池使用率较高，执行紧急重置")
        emergency_pg_pool_reset()
    
    # 检查MySQL连接
    try:
        conn = get_mysql_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            release_mysql_connection(conn)
            db_health_status['mysql'] = True
            logger.info("MySQL连接健康")
    except Exception as e:
        db_health_status['mysql'] = False
        logger.error(f"MySQL连接健康检查失败: {str(e)}")
    
    # 检查PostgreSQL连接
    try:
        conn = get_pg_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            release_pg_connection(conn)
            db_health_status['postgres'] = True
            logger.info("PostgreSQL连接健康")
    except Exception as e:
        db_health_status['postgres'] = False
        logger.error(f"PostgreSQL连接健康检查失败: {str(e)}")
        # 连接失败时尝试重置连接池
        emergency_pg_pool_reset()
    
    return db_health_status

def get_mysql_connection(max_retries=3):
    """从连接池获取MySQL连接，带重试机制，确保旧连接被正确关闭"""
    retry_count = 0
    last_error = None
    old_conn = None
    
    while retry_count < max_retries:
        try:
            # 尝试从连接池获取连接
            if 'mysql_pool' in globals():
                conn = mysql_pool.get_connection()
                # 标记这是一个池连接
                setattr(conn, '_is_pooled', True)
                
                # 测试连接是否可用
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                
                if retry_count > 0:
                    logger.info(f"MySQL连接池重连成功(尝试 {retry_count+1}/{max_retries})")
                return conn
            else:
                # 如果连接池不可用，使用普通连接
                conn = mysql.connector.connect(**MYSQL_CONFIG)
                setattr(conn, '_is_pooled', False)
                
                if retry_count > 0:
                    logger.info(f"MySQL直接连接成功(尝试 {retry_count+1}/{max_retries})")
                return conn
        except Exception as e:
            retry_count += 1
            last_error = e
            
            # 处理上一次尝试的连接对象，确保它被关闭
            if 'conn' in locals() and conn:
                try:
                    if hasattr(conn, 'is_connected') and conn.is_connected():
                        conn.close()
                        logger.debug("关闭失败的MySQL连接")
                except Exception as close_err:
                    logger.warning(f"关闭失败的MySQL连接出错: {str(close_err)}")
            
            wait_time = 2 ** retry_count  # 指数退避
            logger.error(f"MySQL连接失败(尝试 {retry_count}/{max_retries}): {str(e)}")
            
            # 如果还有重试机会，等待后重试
            if retry_count < max_retries:
                logger.info(f"将在{wait_time}秒后重试MySQL连接...")
                time.sleep(wait_time)
    
    # 连接全部失败
    logger.error(f"达到最大重试次数({max_retries})，无法连接到MySQL数据库")
    raise last_error

def release_mysql_connection(conn):
    """释放MySQL连接回连接池，处理不同类型的连接"""
    if not conn:
        return
        
    try:
        # 检查是否是从连接池获取的连接
        if hasattr(conn, '_is_pooled') and conn._is_pooled:
            # 连接池连接
            try:
                # 先检查连接是否仍然有效
                if hasattr(conn, 'is_connected') and conn.is_connected():
                    # 重置会话状态，保证连接回收后的干净状态
                    try:
                        conn.reset_session()
                    except:
                        # 如果重置失败，则关闭连接
                        conn.close()
                        logger.debug("重置MySQL会话失败，已关闭连接")
                    else:
                        # 会话重置成功，正常关闭连接返回池
                        conn.close()
                        logger.debug("MySQL连接已重置并返回连接池")
                else:
                    # 连接已经断开，尝试关闭
                    conn.close()
                    logger.warning("释放一个已断开的MySQL连接")
            except Exception as e:
                logger.error(f"释放MySQL池连接失败: {str(e)}")
        else:
            # 直接创建的连接
            try:
                if hasattr(conn, 'is_connected') and conn.is_connected():
                    conn.close()
                    logger.debug("关闭MySQL直接连接")
            except Exception as e:
                logger.error(f"关闭MySQL直接连接失败: {str(e)}")
    except Exception as e:
        logger.error(f"释放MySQL连接时发生未预期的错误: {str(e)}")

def get_pg_connection(max_retries=3):
    """从连接池获取PostgreSQL连接，带重试机制，确保旧连接被正确关闭"""
    retry_count = 0
    last_error = None
    
    while retry_count < max_retries:
        try:
            # 尝试从连接池获取连接
            if 'pg_pool' in globals():
                conn = pg_pool.getconn()
                # 标记这是一个池连接 - 使用安全的方式设置属性
                try:
                    setattr(conn, '_is_pooled', True)
                except:
                    # 如果无法设置属性，使用字典来跟踪连接
                    if not hasattr(globals(), '_pg_pooled_connections'):
                        globals()['_pg_pooled_connections'] = set()
                    globals()['_pg_pooled_connections'].add(id(conn))
                
                # 测试连接是否可用
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                
                if retry_count > 0:
                    logger.info(f"PostgreSQL连接池重连成功(尝试 {retry_count+1}/{max_retries})")
                return conn
            else:
                # 如果连接池不可用，使用普通连接
                conn = psycopg2.connect(**PG_CONFIG)
                # 标记为非池连接
                try:
                    setattr(conn, '_is_pooled', False)
                except:
                    pass
                
                if retry_count > 0:
                    logger.info(f"PostgreSQL直接连接成功(尝试 {retry_count+1}/{max_retries})")
                return conn
        except Exception as e:
            retry_count += 1
            last_error = e
            
            # 处理上一次尝试的连接对象，确保它被关闭
            if 'conn' in locals() and conn:
                try:
                    if not getattr(conn, 'closed', False):
                        # 检查是否为池连接
                        is_pooled = False
                        try:
                            is_pooled = getattr(conn, '_is_pooled', False)
                        except:
                            # 尝试从跟踪集合中查找
                            if hasattr(globals(), '_pg_pooled_connections'):
                                is_pooled = id(conn) in globals()['_pg_pooled_connections']
                                
                        if is_pooled and 'pg_pool' in globals():
                            try:
                                # 不再使用连接，告诉连接池丢弃它
                                pg_pool.putconn(conn, close=True)
                                logger.debug("通知连接池丢弃损坏的PostgreSQL连接")
                                # 从跟踪集合中移除
                                if hasattr(globals(), '_pg_pooled_connections') and id(conn) in globals()['_pg_pooled_connections']:
                                    globals()['_pg_pooled_connections'].remove(id(conn))
                            except:
                                try:
                                    conn.close()
                                except:
                                    pass
                        else:
                            try:
                                conn.close()
                                logger.debug("关闭失败的PostgreSQL直接连接")
                            except:
                                pass
                except Exception as close_err:
                    logger.warning(f"关闭失败的PostgreSQL连接出错: {str(close_err)}")
            
            wait_time = 2 ** retry_count  # 指数退避
            logger.error(f"PostgreSQL连接失败(尝试 {retry_count}/{max_retries}): {str(e)}")
            
            # 如果还有重试机会，等待后重试
            if retry_count < max_retries:
                logger.info(f"将在{wait_time}秒后重试PostgreSQL连接...")
                time.sleep(wait_time)
    
    # 连接全部失败
    logger.error(f"达到最大重试次数({max_retries})，无法连接到PostgreSQL数据库")
    raise last_error

def release_pg_connection(conn):
    """释放PostgreSQL连接回连接池，处理不同类型的连接"""
    if not conn:
        return
        
    try:
        # 检查是否是从连接池获取的连接
        is_pooled = False
        try:
            is_pooled = getattr(conn, '_is_pooled', False)
        except:
            # 尝试从跟踪集合中查找
            if hasattr(globals(), '_pg_pooled_connections'):
                is_pooled = id(conn) in globals()['_pg_pooled_connections']
        
        if is_pooled:
            # 池连接
            if 'pg_pool' in globals():
                try:
                    # 简化连接测试，只检查是否关闭
                    if not getattr(conn, 'closed', True):
                        # 连接有效，直接返回池
                        pg_pool.putconn(conn)
                        logger.debug("PostgreSQL连接已返回连接池")
                        
                        # 从跟踪集合中移除
                        if hasattr(globals(), '_pg_pooled_connections') and id(conn) in globals()['_pg_pooled_connections']:
                            globals()['_pg_pooled_connections'].remove(id(conn))
                    else:
                        # 连接已关闭，不返回池
                        logger.warning("尝试释放已关闭的PostgreSQL连接")
                except Exception as e:
                    # 连接有问题，关闭它而不是返回池
                    try:
                        if not getattr(conn, 'closed', True):
                            pg_pool.putconn(conn, close=True)  # 放回池但标记为关闭
                            logger.warning(f"PostgreSQL连接有问题，标记为关闭: {str(e)}")
                            
                            # 从跟踪集合中移除
                            if hasattr(globals(), '_pg_pooled_connections') and id(conn) in globals()['_pg_pooled_connections']:
                                globals()['_pg_pooled_connections'].remove(id(conn))
                    except:
                        try:
                            conn.close()
                        except:
                            pass
                        logger.error("关闭问题PostgreSQL连接失败")
            else:
                try:
                    if not getattr(conn, 'closed', True):
                        conn.close()
                except Exception as e:
                    logger.error(f"关闭PostgreSQL连接失败: {str(e)}")
        else:
            # 直接创建的连接
            try:
                if not getattr(conn, 'closed', True):
                    conn.close()
                    logger.debug("关闭PostgreSQL直接连接")
            except Exception as e:
                logger.error(f"关闭PostgreSQL直接连接失败: {str(e)}")
    except Exception as e:
        logger.error(f"释放PostgreSQL连接时发生未预期的错误: {str(e)}")

# 添加紧急修复函数来释放所有PostgreSQL连接
def emergency_pg_pool_reset():
    """紧急重置PostgreSQL连接池，释放所有连接"""
    if 'pg_pool' in globals():
        try:
            # 尝试获取连接池使用情况
            used_count = 0
            if hasattr(pg_pool, '_used'):
                used_count = len(pg_pool._used)
            
            logger.warning(f"执行PostgreSQL连接池紧急重置 (当前使用连接: {used_count})")
            
            # 关闭并重新创建连接池
            try:
                pg_pool.closeall()
            except Exception as e:
                logger.error(f"关闭PostgreSQL连接池失败: {str(e)}")
            
            # 重新创建连接池
            try:
                pg_pool_config = PG_CONFIG.copy()
                globals()['pg_pool'] = psycopg2.pool.ThreadedConnectionPool(
                    minconn=3,  # 增加最小连接数
                    maxconn=20,  # 增加最大连接数
                    user=pg_pool_config['user'],
                    password=pg_pool_config['password'],
                    host=pg_pool_config['host'],
                    port=pg_pool_config['port'],
                    database=pg_pool_config['dbname']
                )
                # 清空跟踪集合
                if hasattr(globals(), '_pg_pooled_connections'):
                    globals()['_pg_pooled_connections'] = set()
                    
                logger.info("PostgreSQL连接池已重置")
                return True
            except Exception as e:
                logger.error(f"重新创建PostgreSQL连接池失败: {str(e)}")
        except Exception as e:
            logger.error(f"PostgreSQL连接池紧急重置失败: {str(e)}")
    return False

app = Flask(__name__)

# 全局变量，存储监控状态
monitor_state = {
    'is_monitoring': False,
    'last_workflow_id': None,
    'countdown_start': None,
    'countdown_minutes': 1,  # 默认倒计时1分钟
    'monitor_thread': None,
    'last_processed_workflow_id': None,  # 替换processed_workflow_ids集合，只存储最近处理的workflow_id
    'thread_heartbeat': datetime.now(),  # 新增：线程心跳时间
    'thread_healthy': True  # 新增：线程健康状态
}

def check_thread_health():
    """检查监控线程的健康状态"""
    global monitor_state
    if not monitor_state['is_monitoring']:
        return True
    
    current_time = datetime.now()
    # 如果超过5分钟没有心跳更新，认为线程不健康
    if (current_time - monitor_state['thread_heartbeat']).total_seconds() > 300:  # 5分钟
        monitor_state['thread_healthy'] = False
        logger.error("监控线程超过5分钟未更新心跳，可能已经死亡")
        return False
    
    return True

def get_latest_workflow_info(max_retries=3):
    """获取最新的workflow_id和最后更新时间"""
    conn = None
    cursor = None
    
    try:
        conn = get_mysql_connection(max_retries=max_retries)
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
        
        if result:
            return result['workflow_id'], result['latest_update']
        return None, None
    except Exception as e:
        logger.error(f"获取最新workflow_id失败: {str(e)}")
        return None, None
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_mysql_connection(conn)

def get_news_count_by_workflow(workflow_id, max_retries=3):
    """获取指定workflow_id的新闻条目数量"""
    conn = None
    cursor = None
    
    try:
        conn = get_mysql_connection(max_retries=max_retries)
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
        
        if result:
            return result[0]
        return 0
    except Exception as e:
        logger.error(f"获取workflow_id {workflow_id}的新闻数量失败: {str(e)}")
        return 0
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_mysql_connection(conn)

def monitoring_thread():
    """监控线程，检查数据更新并倒计时处理"""
    global monitor_state
    
    last_workflow_id = monitor_state['last_workflow_id']
    last_news_count = 0
    countdown_start = None
    no_update_timer = None
    
    logger.info(f"开始监控，倒计时窗口设置为{monitor_state['countdown_minutes']}分钟")
    
    while monitor_state['is_monitoring']:
        try:
            # 更新线程心跳
            monitor_state['thread_heartbeat'] = datetime.now()
            monitor_state['thread_healthy'] = True
            
            # 定期检查数据库连接健康状态
            check_db_connection_health()
            
            # 获取最新的workflow_id和时间戳
            current_workflow_id, update_time = get_latest_workflow_info()
            
            if not current_workflow_id:
                logger.warning("未找到任何workflow，等待30秒后重试")
                time.sleep(30)
                continue
            
            # 检查是否已经处理过该workflow_id
            if current_workflow_id == monitor_state['last_processed_workflow_id']:
                logger.info(f"Workflow_id: {current_workflow_id} 已处理过，等待新的workflow_id")
                time.sleep(60)  # 等待1分钟后再次检查
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
                workflow_success = False
                try:
                    workflow_success = process_workflow(current_workflow_id)
                    if workflow_success:
                        logger.info(f"成功处理workflow_id: {current_workflow_id}")
                        
                        # 添加内容审核处理
                        logger.info(f"开始执行内容审核处理，workflow_id: {current_workflow_id}")
                        content_success = process_content_review(current_workflow_id)
                        if content_success:
                            logger.info(f"成功完成内容审核处理，workflow_id: {current_workflow_id}")
                        else:
                            logger.error(f"内容审核处理失败，workflow_id: {current_workflow_id}")
                        
                        # 处理成功后，更新最后处理的workflow_id
                        monitor_state['last_processed_workflow_id'] = current_workflow_id
                        logger.info(f"更新最后处理的workflow_id为: {current_workflow_id}")
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
            
        except Exception as e:
            logger.error(f"监控线程发生异常: {str(e)}")
            # 发生异常时，等待一段时间后继续
            time.sleep(60)
            continue

@app.route('/api/status', methods=['GET'])
def api_status():
    """API状态检查"""
    # 检查线程健康
    is_thread_healthy = check_thread_health()
    # 检查数据库连接健康
    db_status = check_db_connection_health()
    
    return jsonify({
        'status': 'online',
        'is_monitoring': monitor_state['is_monitoring'],
        'last_workflow_id': monitor_state['last_workflow_id'],
        'countdown_start': monitor_state['countdown_start'].strftime('%Y-%m-%d %H:%M:%S') if monitor_state['countdown_start'] else None,
        'countdown_minutes': monitor_state['countdown_minutes'],
        'thread_healthy': is_thread_healthy,
        'db_health': db_status,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

@app.route('/api/workflows/latest', methods=['GET'])
def get_latest_workflow():
    """获取最新的workflow信息"""
    try:
        workflow_id, update_time = get_latest_workflow_info(max_retries=3)
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
            # 添加内容审核处理
            logger.info(f"开始执行内容审核处理，workflow_id: {workflow_id}")
            content_success = process_content_review(workflow_id)
            if content_success:
                logger.info(f"成功完成内容审核处理，workflow_id: {workflow_id}")
                # 更新已处理的workflow_id
                monitor_state['last_processed_workflow_id'] = workflow_id
                return jsonify({
                    'success': True,
                    'message': f"成功处理workflow: {workflow_id}，包括去重和内容审核"
                })
            else:
                logger.error(f"内容审核处理失败，workflow_id: {workflow_id}")
                return jsonify({
                    'success': False,
                    'message': f"处理workflow: {workflow_id} 成功，但内容审核失败"
                }), 500
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
            # 添加内容审核处理
            logger.info(f"开始执行内容审核处理，workflow_id: {workflow_id}")
            content_success = process_content_review(workflow_id)
            if content_success:
                logger.info(f"成功完成内容审核处理，workflow_id: {workflow_id}")
                # 更新已处理的workflow_id
                monitor_state['last_processed_workflow_id'] = workflow_id
                return jsonify({
                    'success': True,
                    'message': f"成功处理workflow: {workflow_id}，包括去重和内容审核"
                })
            else:
                logger.error(f"内容审核处理失败，workflow_id: {workflow_id}")
                return jsonify({
                    'success': False,
                    'message': f"处理workflow: {workflow_id} 成功，但内容审核失败"
                }), 500
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
                
        # 重置最后处理的workflow_id（可选）
        if 'reset_processed' in data and data['reset_processed']:
            monitor_state['last_processed_workflow_id'] = None
            logger.info("已重置最后处理的workflow_id记录")
        
        # 启动监控线程
        monitor_state['is_monitoring'] = True
        monitor_state['thread_heartbeat'] = datetime.now()
        monitor_state['thread_healthy'] = True
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

@app.route('/api/monitor/reset', methods=['POST'])
def reset_processed_workflows():
    """重置最后处理的workflow_id"""
    global monitor_state
    
    try:
        monitor_state['last_processed_workflow_id'] = None
        
        return jsonify({
            'success': True,
            'message': '已重置处理记录，所有workflow_id将被重新处理'
        })
    except Exception as e:
        logger.error(f"重置处理记录时出错: {str(e)}")
        return jsonify({
            'success': False,
            'message': f"重置处理记录时出错: {str(e)}"
        }), 500

@app.route('/api/check_health', methods=['GET'])
def check_health():
    """
    健康检查接口，用于Docker容器健康监控
    """
    try:
        # 检查MySQL连接
        cnx = mysql_pool.get_connection()
        cnx.close()
        
        # 检查PostgreSQL连接
        pg_conn = pg_pool.getconn()
        pg_pool.putconn(pg_conn)
        
        return jsonify({
            "status": "online",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "message": "服务正常运行"
        }), 200
    except Exception as e:
        logging.error(f"健康检查失败: {e}")
        return jsonify({
            "status": "error",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "message": f"服务异常: {str(e)}"
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
            },
            {
                'url': '/api/monitor/reset',
                'method': 'POST',
                'description': '重置已处理的workflow_id列表',
                'curl_example': 'curl -X POST http://localhost:5001/api/monitor/reset'
            },
            {
                'url': '/api/check_health',
                'method': 'GET',
                'description': '检查系统健康状态',
                'curl_example': 'curl http://localhost:5001/api/check_health'
            }
        ]
    }
    
    return jsonify(docs)

# 关闭连接池的函数
def close_connection_pools():
    """关闭所有数据库连接池，确保所有连接被正确释放"""
    try:
        # 关闭PostgreSQL连接池
        if 'pg_pool' in globals():
            # 获取连接池统计信息
            try:
                used_conns = len(pg_pool._used) if hasattr(pg_pool, '_used') else 'unknown'
                logger.info(f"关闭PostgreSQL连接池前状态: 使用中连接 {used_conns}")
            except:
                pass
                
            # 关闭连接池
            pg_pool.closeall()
            logger.info("PostgreSQL连接池已关闭")
            
        # 关闭MySQL连接池
        # MySQL连接池通常会自动关闭，但我们可以尝试释放所有连接
        if 'mysql_pool' in globals():
            try:
                # 尝试获取连接池统计信息
                logger.info("正在关闭MySQL连接池")
                # 注意：mysql.connector可能没有提供直接关闭池的方法
                # 此处依赖垃圾回收机制
            except Exception as e:
                logger.error(f"在关闭MySQL连接池时遇到问题: {str(e)}")
    except Exception as e:
        logger.error(f"关闭数据库连接池时出错: {str(e)}")

# 程序退出时关闭连接池
import atexit
atexit.register(close_connection_pools)

if __name__ == '__main__':
    # 启动时执行健康检查
    check_db_connection_health()
    
    # 如果连接池初始化失败，程序启动时尝试再次初始化
    if 'mysql_pool' not in globals() or 'pg_pool' not in globals():
        logger.info("程序启动时尝试重新初始化连接池...")
        initialize_connection_pools(max_retries=3)
        
    # 启动Flask应用
    app.run(debug=True, host='0.0.0.0', port=5001) 