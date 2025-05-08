import glob
import tushare as ts
import pandas as pd
import os
import time
import sys
from multiprocessing import Pool
from datetime import datetime
from dotenv import load_dotenv

# 加载.env环境变量
load_dotenv()
TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN')
if not TUSHARE_TOKEN:
    raise ValueError('请在.env文件中设置TUSHARE_TOKEN')

def fetch_and_save_single_stock(args):
    """
    拉取单只股票的历史数据并返回 DataFrame。

    :param args: 包含以下参数的元组：
        - code: 股票代码
        - start_date: 起始日期
        - end_date: 结束日期
        - token: Tushare API 的 Token
        :return: 单只股票的历史数据 DataFrame 或 None
    """
    code, start_date, end_date, token = args
    try:
        ts.set_token(token)
        pro = ts.pro_api()

        # 获取股票数据
        data = pro.daily(ts_code=code, start_date=start_date, end_date=end_date)
        time.sleep(0.4)

        # 检查数据是否为空
        if data is not None and not data.empty:
            data['ts_code'] = code
            return data
        else:
            return None
    except Exception as e:
        return str(e)  # 返回错误信息

def fetch_and_save_stock_data_parallel(stock_codes, start_date, end_date, token, output_file, num_processes):
    """
    使用多进程拉取股票数据并保存为单个 CSV 文件。

    :param stock_codes: 股票代码列表
    :param start_date: 起始日期，格式：YYYYMMDD
    :param end_date: 结束日期，格式：YYYYMMDD
    :param token: Tushare 的 API Token
    :param output_file: 输出 CSV 文件路径
    :param num_processes: 使用的进程数量
    """
    # 为每只股票构建参数元组
    args_list = [(code, start_date, end_date, token) for code in stock_codes]
    
    with Pool(num_processes) as pool:
        total_stocks = len(stock_codes)
        completed = 0
        failed = 0
        successful = 0
        start_time = time.time()

        all_data = []
        
        # 使用 imap_unordered 来获得各个进程返回的数据
        for data in pool.imap_unordered(fetch_and_save_single_stock, args_list):
            if isinstance(data, pd.DataFrame):
                all_data.append(data)
                successful += 1
            else:
                failed += 1

            completed += 1
            
            # 计算当前进度百分比
            percent_complete = (completed / total_stocks) * 100
            elapsed_time = time.time() - start_time
            
            # 每次更新进度时，使用 '\r' 让光标回到行首并更新所有信息
            sys.stdout.write(f"\r拉取进度：{completed}/{total_stocks} ({percent_complete:.1f}%)，成功：{successful}，失败：{failed}，" 
                            f"当前拉取：{args_list[completed-1][0]}，已耗时：{elapsed_time:.2f} 秒")
            sys.stdout.flush()

        # 输出最终换行
        print()
        
        # 合并所有数据
        if all_data:
            final_data = pd.concat(all_data, ignore_index=True)
            output_file_with_date = os.path.join(output_file, f"merged_stocks_data_{end_date}.csv")
            final_data.to_csv(output_file_with_date, index=False)
            print(f"所有数据已保存到 {output_file_with_date}")
        else:
            print("没有数据可以保存。")

        total_elapsed = time.time() - start_time
        print(f"\n最终结果：成功 {successful}，失败 {failed}，总共 {total_stocks} 只股票。总耗时：{total_elapsed:.2f} 秒")

if __name__ == "__main__":
    directory = './data/'
    file_pattern = os.path.join(directory, "基础数据_预处理*.csv")
    files = glob.glob(file_pattern)
    if not files:
        print("没有找到符合条件的文件")
    else:
        latest_file = max(files, key=lambda x: os.path.basename(x).split('_')[-1].replace('.csv', ''))
        stock_list = pd.read_csv(latest_file)
        print(f"读取的文件是: {latest_file}")
    stock_codes = stock_list['ts_code'].values    
    selected_token = TUSHARE_TOKEN
    start_date = '20100101'
    end_date = datetime.today().strftime('%Y%m%d')
    output_file = './data'
    num_processes = 5

    # 开始拉取并保存数据
    fetch_and_save_stock_data_parallel(stock_codes, start_date, end_date, selected_token, output_file, num_processes)