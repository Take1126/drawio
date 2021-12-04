import numpy as np
import pandas as pd
import math
import datetime as dt
from datetime import time
import time
import os
import os.path
from glob import glob
import copy
import re
import sys
import pathlib

#-----------------------------
# main
#-----------------------------
def main():

    # ディレクトリを指定
    power_consumption_dirname = "./Energy"

    # SMA140BとSMA140Cの電力実績データを結合
    power_file_path1 = power_consumption_dirname + "/SMA140B_Enegy_0_1612757156518506.tsv"
    power_file_path2 = power_consumption_dirname + "/SMA140C_Enegy_0_1612757156518506.tsv"
    power_agg_output_filepath = power_consumption_dirname + "/SMA140A_Enegy_0_1612757156518506.tsv"
    power_agg_df = calc_power_agg(power_file_path1, power_file_path2,'SMA140A')
    print(power_agg_df)
    power_agg_df.to_csv(power_agg_output_filepath, sep='\t', index = False, header=False,encoding="shift_jis")

#-----------------------------
# 電力消費量実績データを結合する
#-----------------------------
def calc_power_agg(file_path1, file_path2,production_resource_name):

    out_df = pd.DataFrame(columns = ['DateTime','ProductionResource','PowerConsumption(ACC)','PowerConsumption(INC)'])

    df1 = pd.read_table(file_path1, encoding = 'shift-jis', names=('DateTime', 'ProductionResource','PowerConsumption(ACC)','PowerConsumption(INC)'))
    df2 = pd.read_table(file_path2, encoding = 'shift-jis', names=('DateTime', 'ProductionResource','PowerConsumption(ACC)','PowerConsumption(INC)'))

    out_df['DateTime'] = df1['DateTime'].copy()
    out_df['ProductionResource'] = production_resource_name
    out_df['PowerConsumption(ACC)'] = df1['PowerConsumption(ACC)'] + df2['PowerConsumption(ACC)']
    out_df['PowerConsumption(INC)'] = df1['PowerConsumption(INC)'] + df2['PowerConsumption(INC)']
    
    return out_df

# 直接実行されたとき、メイン関数呼び出し
if __name__ == '__main__':
    start = time.time()
    main()
    process_time = time.time() - start
    print("処理時間: {0}".format(process_time) + "[sec]")
