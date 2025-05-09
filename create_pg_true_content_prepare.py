import psycopg2
import logging
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
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

# 数据库连接配置
PG_CONFIG = {
    'host': '47.86.227.107',
    'port': 5432,
    'user': 'postgres',
    'password': 'root_password',
    'dbname': 'postgres'  # 默认连接到postgres数据库
}

# 创建表格的SQL语句
CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS true_content_prepare (
        link_id VARCHAR(50) PRIMARY KEY,
        workflow_id VARCHAR(50) NOT NULL,
        similarity_notes TEXT,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """
]

# 创建索引的SQL语句
CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_true_content_prepare_workflow_id ON true_content_prepare(workflow_id);"
]

def create_tables():
    """创建PostgreSQL表格"""
    conn = None
    try:
        # 连接到PostgreSQL
        logger.info("正在连接到PostgreSQL...")
        conn = psycopg2.connect(**PG_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # 创建表格
        logger.info("开始创建true_content_prepare表格...")
        for sql in CREATE_TABLES_SQL:
            cursor.execute(sql)
            logger.info(f"已执行SQL: {sql.strip().split('(')[0]}")
        
        # 创建索引
        logger.info("开始创建索引...")
        for sql in CREATE_INDEXES_SQL:
            cursor.execute(sql)
            logger.info(f"已执行SQL: {sql}")
        
        logger.info("true_content_prepare表格和索引创建完成！")
        
    except Exception as e:
        logger.error(f"创建表格失败: {str(e)}")
    finally:
        if conn:
            conn.close()
            logger.info("PostgreSQL连接已关闭")

if __name__ == "__main__":
    create_tables() 