# 📈 Tushare-StockData-Pipeline：量化数据工管道

```mermaid
graph LR
    A([📥 拉取基础数据]) --> B([📊 拉取日线数据])
    B([📊 拉取日线数据]) --> C([🧹 数据清洗])
    C([🧹 数据清洗]) --> D([⏳ 生成多周期数据])
    D([⏳ 生成多周期数据]) --> E([💾 导入数据库])
```

## 📝 项目简介
Tushare-StockData-Pipeline 是一个面向量化投资和数据分析场景的自动化股票数据采集与处理工具。项目基于 Tushare 数据接口，支持批量拉取 A 股市场的基础信息和历史行情数据，自动完成数据清洗、周期转换，并一键导入 PostgreSQL 数据库。适合金融数据研究、策略开发、教学演示等多种用途。

本项目设计注重自动化和可扩展性，用户只需简单配置，即可实现从数据采集到数据库入库的全流程自动化。支持定时任务、批量处理和多周期数据生成，极大提升数据处理效率。


## 🧩 项目结构与工作流程


```
StockData/
├── data/                   # 存放中间和结果数据（不上传数据文件）
├── src/                    # 主程序源码目录
│   ├── main.py             # 主流程控制脚本
│   ├── fetch_base_data.py  # 拉取股票基础信息
│   ├── clean_data.py       # 数据清洗与预处理
│   ├── fetch_daily_data.py # 拉取日线历史行情数据
│   ├── generate_periodic_data.py # 生成多周期（日/周/月/季/年）数据
│   ├── upload_database.py  # 数据导入PostgreSQL数据库
│   └── ...                 # 其他辅助脚本
├── run.sh                  # 一键批量运行脚本
├── requirements.txt        # 依赖包列表
├── .env.example            # 环境变量示例
└── README.md               # 项目说明文档
```

### 工作流程说明
1. 运行 `run.sh` 或 `src/main.py` 启动主流程。
2. 依次拉取基础数据、历史行情数据。
3. 对原始数据进行清洗、去重、预处理。
4. 生成多周期数据。
5. 将处理后的数据批量导入 PostgreSQL 数据库。

---

## 🚀 主要功能
- 自动拉取股票基础信息、曾用名、新股上市等数据
- 日线历史行情数据批量拉取与合并
- 数据清洗、去重、预处理
- 多周期数据（日、周、月、季、年）自动生成
- 数据批量导入PostgreSQL数据库

## 🛠️ 依赖环境
- Python 3.11（推荐使用conda创建环境）
- 依赖包见 `requirements.txt`
- 需注册Tushare账号，并在`.env.example`文件中填写TUSHARE_TOKEN后，复制为`.env`

## 📚 基础依赖
本项目主要基于 [Tushare](https://tushare.pro/) 数据接口进行股票数据采集与处理。

## ⚡ 使用方法 Mac/Linux
1. 使用conda创建并激活Python 3.11环境：
   ```bash
   conda create -n stockdata python=3.11 -y
   conda activate stockdata
   ```
2. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```
3. 编辑.env文件：
   ```bash
   cp .env.example .env
   ```
4. 运行主程序：
   ```bash
   python src/main.py
   ```

## ⏰ 批量与定时运行
本项目提供了 `run.sh` 脚本用于批量运行主流程。你可以直接在 Mac 或 Linux 终端执行：

```bash
bash run.sh
```

如需定时运行（如每周1-5 17点自动执行），可使用 crontab：

```bash
crontab -e
```
添加如下内容：
```
0 17 * * 1-5 /bin/bash /your_path/StockData/run.sh
```
这样即可实现自动化定时任务。

## 📁 数据目录说明
本项目的 `data/` 目录用于存放中间数据和结果数据。为保护隐私和节省空间，`data/` 目录下的数据文件不会上传到仓库，仅保留空目录（通过 `.gitkeep` 文件）。如需使用，请自行在本地添加数据文件。

## 📅 数据默认拉取时间范围
本项目默认拉取的数据时间范围为：**2010-01-01 至最新交易日**。

如需修改拉取时间范围，前往 `src/Pull_merga_stock.py` 

```python
start_date = '2010-01-01'
```

## ⚠️ 注意事项
- 需先启动本地PostgreSQL数据库并创建名为`stocks`的数据库。
- 数据量较大时，拉取和导入过程可能耗时较长。

---

## 📝 开源协议
本项目采用 MIT License 开源协议，欢迎自由使用和二次开发。

如有问题请联系作者或提交issue。
