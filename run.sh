#!/bin/bash
cd "$(dirname "$0")"  # 切换到脚本所在目录

source /opt/anaconda3/etc/profile.d/conda.sh
conda activate stockdata

python ./src/main.py