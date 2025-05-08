import pandas as pd
import os
import glob
import sys
import time

# 获取主脚本拉取的 CSV 文件名
directory = './data/'
file_pattern = os.path.join(directory, "merged_stocks_data_*.csv")
files = glob.glob(file_pattern)

if not files:
    print("没有找到符合条件的文件")
else:
    latest_file = max(files, key=lambda x: os.path.basename(x).split('_')[-1].replace('.csv', ''))
    stock_list = pd.read_csv(latest_file)
    print(f"读取的文件是: {latest_file}")

# 检查是否已经存在 'cycle' 列，若存在，则跳过周期数据的生成
if 'cycle' in stock_list.columns:
    print("周期数据存在，跳过操作。")
else:
    print("多周期数据生成中......")

    # 读取日线数据，并统一日期格式
    daily_data = pd.read_csv(latest_file)

    # 将 'trade_date' 从 int 转换为字符串格式，并转换为 datetime 类型，统一格式为 '%Y-%m-%d'
    daily_data['trade_date'] = pd.to_datetime(daily_data['trade_date'].astype(str), format='%Y%m%d').dt.strftime('%Y-%m-%d')

    # 按照 ts_code 和 trade_date 排序
    daily_data = daily_data.sort_values(by=['ts_code', 'trade_date'])

    # 为日线数据添加 'cycle' 标签，标记为 'daily'
    daily_data['cycle'] = 'daily'

    # 定义一个函数来生成每个周期的数据，并删除空行
    def resample_data(group, freq, cycle_label, stock_index, total_stocks):
        # 更新进度
        progress = (stock_index / total_stocks) * 100
        sys.stdout.write(f"\r生成{cycle_label}数据: {stock_index}/{total_stocks} 股票 ({progress:.1f}%)")
        sys.stdout.flush()
        
        # 确保 trade_date 是 datetime 格式，并设置为索引
        group['trade_date'] = pd.to_datetime(group['trade_date'], errors='coerce')
        group = group.set_index('trade_date')

        # 对单个股票进行重采样，使用不同周期的结束日期
        resampled_data = group.resample(freq).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'change': 'last',
            'pct_chg': 'last',
            'vol': 'sum',
            'amount': 'sum'
        }).reset_index()

        # 删除没有数据的周期
        resampled_data = resampled_data.dropna(subset=['open', 'high', 'low', 'close', 'vol', 'amount'])

        # 保留 ts_code 字段，并将 ts_code 放在最前面
        resampled_data['ts_code'] = group['ts_code'].iloc[0]

        # 获取每个周期的最后一个交易日（即每周最后一个交易日）
        resampled_data['trade_date'] = resampled_data['trade_date'].dt.to_period(freq).dt.end_time

        # 修正 pre_close，设置为前一个周期的收盘价
        resampled_data['pre_close'] = resampled_data['close'].shift(1)
        
        # 如果 pre_close 是 NaN（即第一个周期），将其设置为 0
        resampled_data['pre_close'] = resampled_data['pre_close'].fillna(0)

        # 计算 change（当前周期的 close 减去前一个周期的 pre_close）
        resampled_data['change'] = resampled_data['close'] - resampled_data['pre_close']

        # 计算 pct_chg，避免除以零，若 pre_close 为 0，则 pct_chg 设置为 0
        resampled_data['pct_chg'] = resampled_data.apply(
            lambda row: (row['close'] - row['pre_close']) / row['pre_close'] * 100 if row['pre_close'] != 0 else 0,
            axis=1
        )

        # 四舍五入到 2 位小数
        resampled_data['change'] = resampled_data['change'].round(2)
        resampled_data['pct_chg'] = resampled_data['pct_chg'].round(2)

        # 统一格式为 '%Y-%m-%d' 字符串
        resampled_data['trade_date'] = resampled_data['trade_date'].dt.strftime('%Y-%m-%d')

        # 调整列顺序，使 'ts_code' 在 'trade_date' 前面
        columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']
        resampled_data = resampled_data[columns]

        # 添加周期列
        resampled_data['cycle'] = cycle_label
        
        return resampled_data

    # 为每只股票分别生成周期数据
    stock_groups = daily_data.groupby('ts_code')
    total_stocks = len(stock_groups)
    start_time = time.time()
    
    print(f"开始生成周期数据，共需处理 {total_stocks} 只股票")
    
    # 生成周线数据，使用每周五作为周期标记
    print("\n生成周线数据...")
    weekly_data = pd.concat([resample_data(group, 'W-FRI', 'weekly', i+1, total_stocks) 
                            for i, (_, group) in enumerate(stock_groups)], ignore_index=True)
    print(f"\n周线数据生成完成，耗时: {time.time() - start_time:.2f} 秒")
    
    # 重置计时器
    interim_time = time.time()
    
    # 生成月线数据
    print("\n生成月线数据...")
    monthly_data = pd.concat([resample_data(group, 'M', 'monthly', i+1, total_stocks) 
                             for i, (_, group) in enumerate(stock_groups)], ignore_index=True)
    print(f"\n月线数据生成完成，耗时: {time.time() - interim_time:.2f} 秒")
    
    # 重置计时器
    interim_time = time.time()
    
    # 生成季线数据
    print("\n生成季线数据...")
    quarterly_data = pd.concat([resample_data(group, 'Q', 'quarterly', i+1, total_stocks) 
                               for i, (_, group) in enumerate(stock_groups)], ignore_index=True)
    print(f"\n季线数据生成完成，耗时: {time.time() - interim_time:.2f} 秒")
    
    # 重置计时器
    interim_time = time.time()
    
    # 生成年线数据
    print("\n生成年线数据...")
    yearly_data = pd.concat([resample_data(group, 'A', 'yearly', i+1, total_stocks) 
                            for i, (_, group) in enumerate(stock_groups)], ignore_index=True)
    print(f"\n年线数据生成完成，耗时: {time.time() - interim_time:.2f} 秒")

    # 合并所有周期数据
    print("\n合并所有周期数据...")
    combined_data = pd.concat([daily_data, weekly_data, monthly_data, quarterly_data, yearly_data], ignore_index=True)

    # 保存为 CSV 文件（覆盖原始文件）
    print(f"保存数据到 {latest_file}...")
    save_start = time.time()
    combined_data.to_csv(latest_file, index=False)
    print(f"保存完成，耗时: {time.time() - save_start:.2f} 秒")

    # 删除已保存的周期数据文件
    for cycle_data, cycle_name in zip([weekly_data, monthly_data, quarterly_data, yearly_data], ['weekly_data.csv', 'monthly_data.csv', 'quarterly_data.csv', 'yearly_data.csv']):
        cycle_file_path = os.path.join(directory, cycle_name)
        if os.path.exists(cycle_file_path):
            os.remove(cycle_file_path)
            print(f"{cycle_name} 已删除")

    total_time = time.time() - start_time
    print(f"\n数据生成完成！共处理 {total_stocks} 只股票，总耗时: {total_time:.2f} 秒")