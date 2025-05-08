import os
import subprocess
import time
from datetime import datetime
import glob
from dotenv import load_dotenv

# 自动加载项目根目录下的 .env 文件
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

def main():
    start_time = time.time()

    # 获取当前日期
    current_date = datetime.now().strftime('%Y%m%d')

    # 定义基础数据文件的目标路径和命名规则
    target_directory = './data'
    base_data_filename = os.path.join(target_directory, f'基础数据_预处理{current_date}.csv')
    daily_data_filename = os.path.join(target_directory, f'merged_stocks_data_{current_date}.csv')

    # 检查基础数据文件是否已存在
    if os.path.isfile(base_data_filename):
        print("--------------------基础数据已存在，跳过采集和清洗步骤，直接进入日线数据处理>>>--------------------")
    else:
        # 如果没有生成文件，则执行基础数据采集和清洗
        print("--------------------开始采集今日基础数据--------------------")
        subprocess.run(['python', os.path.join('src', 'Pull_base_data.py')], check=True)

        print("--------------------采集完成，进入清洗、去重、预处理>>>--------------------")
        subprocess.run(['python', os.path.join('src', 'Clear_data.py')], check=True)

    # 检查日线数据文件是否已存在
    if os.path.isfile(daily_data_filename):
        print(f"--------------------今日日线数据文件已存在：{daily_data_filename}，跳过拉取步骤>>>--------------------")
    else:
        # 如果没有生成日线数据文件，则执行日线数据拉取
        print("--------------------开始拉取日线历史数据--------------------")
        subprocess.run(['python', os.path.join('src', 'Pull_merga_stock.py')], check=True) 

    print("--------------------开始生成周期数据--------------------")
    subprocess.run(['python', os.path.join('src', 'Generating_periodic_data.py')], check=True)

    print("--------------------更新数据库--------------------")
    subprocess.run(['python', os.path.join('src', 'Upload_database.py')], check=True)

    end_time = time.time()
    total_time = end_time - start_time
    print(f"--------------------基础数据、日、周、月、季、年线数据拉取并更新结束，耗时：{total_time:.2f} 秒--------------------")

if __name__ == "__main__":
    main()