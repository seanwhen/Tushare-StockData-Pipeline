import tushare as ts
import pandas as pd
import os
from dotenv import load_dotenv

# 加载.env环境变量
load_dotenv()
TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN')

if not TUSHARE_TOKEN:
    raise ValueError('请在.env文件中设置TUSHARE_TOKEN')

ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

data_dir = './data'
os.makedirs(data_dir, exist_ok=True)

# 获取并保存日线行情数据
def fetch_and_save_daily_data(pro):
    df = pro.daily(ts_code="", trade_date="", start_date="", end_date="", offset="", limit="")
    df_filtered = df[~df['ts_code'].str.startswith(('8', '9'))]
    latest_date = df_filtered['trade_date'].max()
    df_latest = df_filtered[df_filtered['trade_date'] == latest_date]
    file_name = os.path.join(data_dir, f"日线行情{latest_date}.csv")
    df_latest.to_csv(file_name, index=False)
    print(f"日线行情数据形状: {df_latest.shape}")

# 获取并保存上市公司基础信息
def fetch_and_save_stock_company_data(pro, limit=5000):
    offset = 0
    all_data = []
    while True:
        df = pro.stock_company(limit=limit, offset=offset, fields=["ts_code", "chairman", "manager", "reg_capital", "province", "city", "website", "email", "business_scope", "employees", "introduction", "setup_date", "main_business"])
        if df.empty:
            break
        all_data.append(df)
        offset += limit
    all_data_df = pd.concat(all_data, ignore_index=True)
    filtered_df = all_data_df[~all_data_df['ts_code'].str.startswith('8')]
    filtered_df.to_csv(os.path.join(data_dir, '上市公司基本信息.csv'), index=False)
    print(f"上市公司基础信息形状: {filtered_df.shape}")

# 获取并保存股票曾用名数据
def fetch_and_save_namechange_data(pro, limit=5000):
    offset = 0
    all_data = []
    while True:
        df = pro.namechange(ts_code="", start_date="", end_date="", limit=limit, offset=offset)
        if df.empty:
            break
        all_data.append(df)
        offset += limit
    all_data_df = pd.concat(all_data, ignore_index=True)
    filtered_df = all_data_df[~all_data_df['ts_code'].str.startswith(('T', 'A', '9', '8', '7'))]
    filtered_df = filtered_df.sort_values(by='ann_date', ascending=False).drop_duplicates(subset='ts_code', keep='first')
    filtered_df.to_csv(os.path.join(data_dir, '股票曾用名.csv'), index=False)
    print(f"股票曾用名数据形状: {filtered_df.shape}")

# 获取并保存新股上市数据
def fetch_and_save_new_share_data(pro, limit=5000):
    offset = 0
    all_data = []
    while True:
        df = pro.new_share(start_date="", end_date="", limit=limit, offset=offset)
        if df.empty:
            break
        all_data.append(df)
        offset += limit
    all_data_df = pd.concat(all_data, ignore_index=True)
    filtered_df = all_data_df[~all_data_df['ts_code'].str.startswith(('8', '9'))]
    filtered_df.to_csv(os.path.join(data_dir, 'IPO新股上市.csv'), index=False)
    print(f"新股上市数据形状: {filtered_df.shape}")

fetch_and_save_daily_data(pro)
fetch_and_save_stock_company_data(pro)
fetch_and_save_namechange_data(pro)
fetch_and_save_new_share_data(pro)

df = pro.stock_basic(**{"ts_code": "", "name": "", "exchange": "", "market": "", "is_hs": "", "list_status": "", "limit": "", "offset": ""}, fields=["ts_code", "symbol", "name", "area", "industry", "market", "list_date", "act_name", "act_ent_type", "fullname", "enname", "exchange", "is_hs"])
df_filtered = df[~df['ts_code'].str.startswith('8')]
df_filtered.to_csv(os.path.join(data_dir, '股票列表.csv'), index=False)
print(f"股票列表数据形状: {df_filtered.shape}")

df = pro.bak_basic(**{"trade_date": "", "ts_code": "", "limit": "", "offset": ""}, fields=["trade_date", "ts_code", "industry", "area", "pe", "float_share", "total_share", "total_assets", "liquid_assets", "fixed_assets", "reserved", "eps", "bvps", "pb", "list_date", "undp", "per_undp", "rev_yoy", "profit_yoy", "gpr", "npr", "holder_num", "name"])
df_filt = df[~df['ts_code'].str.startswith('8')]
filt_df = df_filt.sort_values(by='trade_date', ascending=False).drop_duplicates(subset='ts_code', keep='first')
filt_df.to_csv(os.path.join(data_dir, '备用列表.csv'), index=False)
print(filt_df.shape)
print(f"备用列表数据形状: {filt_df.shape}")