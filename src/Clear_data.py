import os
import glob
import pandas as pd
from datetime import datetime

data_dir = './data'
os.makedirs(data_dir, exist_ok=True)

# 定义文件列表
files = [
    '备用列表.csv',
    '股票列表.csv',
    '股票曾用名.csv',
    'IPO新股上市.csv',
    '上市公司基本信息.csv'
]

# 添加日线行情
daily_file = glob.glob(os.path.join(data_dir, '日线行情*.csv'))
if daily_file:
    files.extend([os.path.basename(f) for f in daily_file])

# 读取所有文件并设置ts_code作为索引，存储在一个字典中
dfs = {file: pd.read_csv(os.path.join(data_dir, file), index_col='ts_code') for file in files}

# 合并所有DataFrame，按索引进行外连接
merged_df = pd.concat(dfs.values(), axis=1)

# 保存合并后的数据
merged_df.to_csv(os.path.join(data_dir, '基础数据_未清洗.csv'))

# 获取当前日期并格式化为 'YYYYMMDD' 格式
current_date = datetime.now().strftime('%Y%m%d')

# 读取 CSV 文件
df = pd.read_csv(os.path.join(data_dir, '基础数据_未清洗.csv'))

# 添加“流通市值、总市值”计算市值并四舍五入到一位小数，然后添加到新列
df['TMC'] = round(df['total_share'] * df['close'], 1)
df['CMV'] = round(df['float_share'] * df['close'], 1)

# 仅保留以 '6', '3', '0' 开头的代码
filtered_df = df[df['ts_code'].str.startswith(('6', '3', '0')) & ~df['ts_code'].str.startswith('68')]

# 清理标签 'area'地区 或 'industry' 为空值的行
df_cleaned = filtered_df.dropna(subset=['area'])

# 去除包含 'ST' 的行
df_cleaned = df_cleaned[~df_cleaned['name'].str.contains('ST')]

# 去除重复的行，保留最后一个出现的
df_cleaned = df_cleaned.drop_duplicates(subset='ts_code', keep='last')

# 定义所需的列，并按照给定顺序排列
desired_columns = [
    'ts_code', 'name', 'industry', 'fullname', 'area', 'city', 'close', 'TMC', 'CMV',
    'list_date', 'ipo_date','ann_date', 'change_reason', 'act_name', 'act_ent_type', 
    'chairman', 'manager', 'business_scope', 'employees', 'introduction', 
    'main_business', 'total_assets','liquid_assets', 'bvps', 'pb', 
    'undp', 'profit_yoy', 'holder_num'
]

# 提取所需列
df_final = df_cleaned[desired_columns]

output_filename = os.path.join(data_dir, f'基础数据_预处理{current_date}.csv')
df_final.to_csv(output_filename, index=False)
print(f"数据已保存至：{output_filename}")
print(f"数据形状：{df_final.shape}")

# 删除data目录下除新生成的 CSV 文件之外的其他 CSV 文件
csv_files = glob.glob(os.path.join(data_dir, '*.csv'))
for file in csv_files:
    if file != output_filename:
        os.remove(file)

print(f"保存为 '{output_filename}'")