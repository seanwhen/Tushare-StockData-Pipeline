import psycopg2
import pandas as pd
import time
from datetime import datetime
import os
import logging
import io
import sys
import random
import multiprocessing

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def create_database_connection(max_attempts=5, base_delay=5):
    """创建数据库连接，如果失败则重试
    
    Args:
        max_attempts: 最大重试次数
        base_delay: 基础等待时间（秒）
    
    Returns:
        psycopg2连接对象
    
    Raises:
        Exception: 如果超过最大重试次数仍无法连接
    """
    attempt = 0
    last_exception = None
    
    while attempt < max_attempts:
        try:
            conn = psycopg2.connect(
                host='localhost', 
                user='postgres', 
                dbname='stocks',
                connect_timeout=10,
                options='-c statement_timeout=300000'  # 5分钟超时
            )
            if attempt > 0:
                logger.info(f"成功连接到数据库，尝试次数：{attempt+1}")
            return conn
        except psycopg2.OperationalError as e:
            attempt += 1
            last_exception = e
            
            # 检查是否是数据库被占用的错误
            if "is being accessed by other users" in str(e):
                # 使用指数退避和随机抖动
                delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 3)
                logger.warning(f"数据库被其他用户占用，{delay:.2f}秒后重试 ({attempt}/{max_attempts})：{e}")
                time.sleep(delay)
            else:
                # 其他连接错误，直接抛出
                raise e
    
    # 超出最大重试次数
    logger.error(f"无法连接到数据库，已达到最大重试次数：{max_attempts}")
    raise last_exception

def upsert_batch(args):
    batch_data, columns, db_params = args
    try:
        conn = psycopg2.connect(**db_params)
        with conn:
            with conn.cursor() as cursor:
                buffer = io.StringIO()
                batch_data.to_csv(buffer, index=False, header=False)
                buffer.seek(0)
                # 临时表用于copy_from
                cursor.execute("""
                    CREATE TEMP TABLE tmp_stock_data (
                        ts_code VARCHAR(10),
                        trade_date DATE,
                        cycle VARCHAR(10),
                        open FLOAT,
                        high FLOAT,
                        low FLOAT,
                        close FLOAT,
                        pre_close FLOAT,
                        change FLOAT,
                        pct_chg FLOAT,
                        vol FLOAT,
                        amount FLOAT
                    ) ON COMMIT DROP;
                """)
                cursor.copy_from(buffer, 'tmp_stock_data', sep=',', columns=columns)
                # upsert
                cursor.execute(f"""
                    INSERT INTO stock_data ({', '.join(columns)})
                    SELECT {', '.join(columns)} FROM tmp_stock_data
                    ON CONFLICT (ts_code, trade_date, cycle) DO UPDATE SET
                        open=EXCLUDED.open,
                        high=EXCLUDED.high,
                        low=EXCLUDED.low,
                        close=EXCLUDED.close,
                        pre_close=EXCLUDED.pre_close,
                        change=EXCLUDED.change,
                        pct_chg=EXCLUDED.pct_chg,
                        vol=EXCLUDED.vol,
                        amount=EXCLUDED.amount;
                """)
                buffer.close()
        conn.close()
        return len(batch_data)
    except Exception as e:
        logger.error(f"批次上传失败: {e}")
        return 0

def main():
    start_time = time.time()
    
    # 读取CSV
    today = datetime.today().strftime('%Y%m%d')
    csv_file_path = f'./data/merged_stocks_data_{today}.csv'
    
    try:
        data = pd.read_csv(csv_file_path)
        logger.info(f"读取CSV文件，共{len(data)}条数据")
    except Exception as e:
        logger.error(f"读取CSV文件失败: {e}")
        return
    
    # 只保留需要的列，按数据库顺序
    columns = ['ts_code', 'trade_date', 'cycle', 'open', 'high', 'low', 'close', 
               'pre_close', 'change', 'pct_chg', 'vol', 'amount']
    data = data[columns]
    
    # 计算批次数
    batch_size = 100000
    num_rows = len(data)
    num_batches = (num_rows + batch_size - 1) // batch_size
    
    db_params = dict(
        host='localhost', 
        user='postgres', 
        dbname='stocks',
        connect_timeout=10,
        options='-c statement_timeout=300000'
    )
    
    # 使用上下文管理器处理数据库连接，包含重试机制
    conn = None
    try:
        # 尝试建立连接并处理可能的数据库占用问题
        conn = create_database_connection()
        
        with conn:
            with conn.cursor() as cursor:
                # 建表
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS stock_data (
                    ts_code VARCHAR(10) NOT NULL,
                    trade_date DATE NOT NULL,
                    cycle VARCHAR(10) NOT NULL,
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT,
                    pre_close FLOAT,
                    change FLOAT,
                    pct_chg FLOAT,
                    vol FLOAT,
                    amount FLOAT,
                    PRIMARY KEY (ts_code, trade_date, cycle)
                );
                """
                cursor.execute(create_table_sql)
        conn.close()
        
        # 多进程上传
        pool = multiprocessing.Pool(processes=min(4, num_batches))
        batches = [(data.iloc[i*batch_size:(i+1)*batch_size], columns, db_params) for i in range(num_batches)]
        results = []
        for i, res in enumerate(pool.imap_unordered(upsert_batch, batches), 1):
            results.append(res)
            rows_processed = sum(results)
            percent = int((rows_processed/num_rows)*100)
            progress_msg = f"数据库导入进度: {percent}% ({rows_processed}/{num_rows})"
            sys.stdout.write('\r' + progress_msg)
            sys.stdout.flush()
        pool.close()
        pool.join()
        
        # 导入完成后换行
        print()
        
        # 重新分析表
        conn = create_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("ANALYZE stock_data;")
        logger.info(f"成功导入 {num_rows} 条数据！")
                
    except Exception as e:
        logger.error(f"数据库操作失败: {e}")
        # 确保连接被关闭
        if conn and not conn.closed:
            conn.close()
    
    # 显示总耗时
    elapsed_time = time.time() - start_time
    logger.info(f"任务完成，总耗时: {elapsed_time:.2f} 秒")

if __name__ == "__main__":
    main()