# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import re
import sys
import os
import io
import openpyxl
import datetime as dt
from datetime import time
import time
import main

#---------------------------------------------------
# 1時間毎の消費電力量とCO2排出量
#---------------------------------------------------
def electric_power_per_hour_calculate(dict_electric_sheet_all):
    df_electric_power_per_hour = pd.DataFrame()
    df_electric_power_per_hour_temp = pd.DataFrame()
    df_electric_power_temp = pd.DataFrame()
    for key in dict_electric_sheet_all: #ExcelのSheet名称をキーとして個別にSheetの情報を収集する
        #print("================dict_electric_sheet_all[key]====================")
        #print(dict_electric_sheet_all[key])
        if df_electric_power_per_hour.empty:
            df_electric_power_per_hour = dict_electric_sheet_all[key].copy().fillna({'PowerConsumption': 0}) #電力量データの欠損部に0を代入
            #df_electric_power_per_hour.fillna({'PowerConsumption': 0})
            for index, row in df_electric_power_per_hour.iterrows():
                if index == 23: #各Sheetの最終行のみ0:00なので次の日の電力量としてカウントする
                    if type(row['DateTime']) is str:
                        #print("===============index====================")    
                        #print(index)
                        #print("================type(row)====================")    
                        #print(type(row['DateTime']))
                        #date = key.strftime('%Y/%m/%d')
                        time = row['DateTime']
                        datetime_string = key + time
                        datetime = dt.datetime.strptime(datetime_string, '%Y年%m月%d日 %H:%M') + dt.timedelta(days=1)
                        #print("================time====================")
                        #print(time)
                        #print("================datetime_string====================")
                        #print(datetime_string)
                        #print("================datetime====================")
                        #print(datetime)
                        df_electric_power_per_hour.at[index, 'DateTime'] = datetime
                    elif type(row['DateTime']) is dt.time:
                        time = row['DateTime']
                        time_string = time.strftime('%H:%M:%S')
                        datetime_string = key +" "+time_string
                        datetime = dt.datetime.strptime(datetime_string, '%Y年%m月%d日 %H:%M:%S') + dt.timedelta(days=1)
                        #print("================dt.time====================")
                        #print(time)
                        #print("================dt.datetime_string====================")
                        #print(datetime_string)
                        #print("================dt.datetime====================")
                        #print(datetime)
                        #row['DateTime'] = datetime  
                        df_electric_power_per_hour.at[index, 'DateTime'] = datetime
                    
                    else:
                        pass
                else:
                    if type(row['DateTime']) is str:
                        #print("===============index====================")    
                        #print(index)
                        #print("================type(row)====================")    
                        #print(type(row['DateTime']))
                        #date = key.strftime('%Y/%m/%d')
                        time = row['DateTime']
                        datetime_string = key + time
                        datetime = dt.datetime.strptime(datetime_string, '%Y年%m月%d日 %H:%M')
                        #print("================time====================")
                        #print(time)
                        #print("================datetime_string====================")
                        #print(datetime_string)
                        #print("================datetime====================")
                        #print(datetime)
                        df_electric_power_per_hour.at[index, 'DateTime'] = datetime
                    elif type(row['DateTime']) is dt.time:
                        time = row['DateTime']
                        time_string = time.strftime('%H:%M:%S')
                        datetime_string = key +" "+time_string
                        datetime = dt.datetime.strptime(datetime_string, '%Y年%m月%d日 %H:%M:%S')
                        #print("================dt.time====================")
                        #print(time)
                        #print("================dt.datetime_string====================")
                        #print(datetime_string)
                        #print("================dt.datetime====================")
                        #print(datetime)
                        #row['DateTime'] = datetime  
                        df_electric_power_per_hour.at[index, 'DateTime'] = datetime
        else:
            #print("================df_electric_power_per_hour.key====================")
            #print(key)
            df_electric_power_per_hour_temp = df_electric_power_per_hour.copy()
            df_electric_power_temp = dict_electric_sheet_all[key].copy().fillna({'PowerConsumption': 0})
            for index, row in df_electric_power_temp.iterrows():
                if index == 23:
                    if type(row['DateTime']) is str:
                        #print("===============index====================")    
                        #print(index)
                        #print("================type(row)====================")    
                        #print(type(row['DateTime']))
                        #date = key.strftime('%Y/%m/%d')
                        time = row['DateTime']
                        datetime_string = key + time
                        datetime = dt.datetime.strptime(datetime_string, '%Y年%m月%d日 %H:%M') + dt.timedelta(days=1)
                        #print("================time====================")
                        #print(time)
                        #print("================datetime_string====================")
                        #print(datetime_string)
                        #print("================datetime====================")
                        #print(datetime)
                        df_electric_power_temp.at[index, 'DateTime'] = datetime
                    elif type(row['DateTime']) is dt.time:
                        time = row['DateTime']
                        time_string = time.strftime('%H:%M:%S')
                        datetime_string = key +" "+time_string
                        datetime = dt.datetime.strptime(datetime_string, '%Y年%m月%d日 %H:%M:%S') + dt.timedelta(days=1)
                        #print("================dt.time====================")
                        #print(time)
                        #print("================dt.datetime_string====================")
                        #print(datetime_string)
                        #print("================dt.datetime====================")
                        #print(datetime)
                        #row['DateTime'] = datetime  
                        df_electric_power_temp.at[index, 'DateTime'] = datetime
                    
                    else:
                        pass
                else:
                    if type(row['DateTime']) is str:
                        #print("===============index====================")    
                        #print(index)
                        #print("================type(row)====================")    
                        #print(type(row['DateTime']))
                        #date = key.strftime('%Y/%m/%d')
                        time = row['DateTime']
                        datetime_string = key + time
                        datetime = dt.datetime.strptime(datetime_string, '%Y年%m月%d日 %H:%M')
                        #print("================time====================")
                        #print(time)
                        #print("================datetime_string====================")
                        #print(datetime_string)
                        #print("================datetime====================")
                        #print(datetime)
                        df_electric_power_temp.at[index, 'DateTime'] = datetime
                    elif type(row['DateTime']) is dt.time:
                        time = row['DateTime']
                        time_string = time.strftime('%H:%M:%S')
                        datetime_string = key +" "+time_string
                        datetime = dt.datetime.strptime(datetime_string, '%Y年%m月%d日 %H:%M:%S')
                        #print("================dt.time====================")
                        #print(time)
                        #print("================dt.datetime_string====================")
                        #print(datetime_string)
                        #print("================dt.datetime====================")
                        #print(datetime)
                        df_electric_power_temp.at[index, 'DateTime'] = datetime
            df_electric_power_per_hour = pd.concat([df_electric_power_per_hour_temp, df_electric_power_temp], join='inner')
        # CO2排出量換算    
        df_electric_power_per_hour['CO2emissions']= df_electric_power_per_hour['PowerConsumption'] * 0.997 #CO2排出量[kg-CO2/kwh]計算
    return df_electric_power_per_hour

#---------------------------------------------------
# 電力量とCO2のコスト計算
#---------------------------------------------------
def electric_power_cost_per_hour_calculate(df_electric_power_per_hour):
    df_electric_power_per_hour['PowerUnitCost'] = df_electric_power_per_hour['DateTime'].map(lambda x: electric_power_unit_cost(x))
    df_electric_power_per_hour['ElectricPowerCost'] =  df_electric_power_per_hour['PowerConsumption'] * df_electric_power_per_hour['PowerUnitCost']
    df_electric_power_per_hour['CO2emissionsCost'] = df_electric_power_per_hour['CO2emissions'] * 0.935 #CO2排出量取引費用(中国元：55元/t-CO2⇒日本円：935円/t-CO2)
    df_electric_power_per_hour = df_electric_power_per_hour.drop(columns=['PowerUnitCost']).copy()

    return df_electric_power_per_hour
#---------------------------------------------------
# 時間（DateTime）を入力として電力単価を返す
#---------------------------------------------------
def electric_power_unit_cost(dtime):
    unit_price = None
    # 2021/8/26 以下6行分コメント化
    # t1 = dt.datetime.strptime('00:00:00', '%H:%M:%S')
    # t2 = dt.datetime.strptime('08:00:00', '%H:%M:%S')
    # t3 = dt.datetime.strptime('10:00:00', '%H:%M:%S')
    # t4 = dt.datetime.strptime('17:00:00', '%H:%M:%S')
    # t5 = dt.datetime.strptime('22:00:00', '%H:%M:%S')
    # t = dt.datetime(t1.year, t1.month, t1.day, dtime.hour, dtime.minute, dtime.second)

    # 2021/8/26 以下15行分コメント化
    # # 夜間時間
    # if t1 <= t < t2:
    #     unit_price = 13.66
    # # 昼間時間
    # elif t2 <= t < t3:
    #     unit_price = 16.21
    # # 重負荷時間
    # elif t3 <= t < t4:
    #     unit_price = 19.01
    # # 昼間時間
    # elif t4 <= t < t5:
    #     unit_price = 16.21
    # # デフォルト
    # else:
    #     unit_price = 13.66

    # 電力量原単位用に月別に判定できるように加工(2021/8/26 追加)
    t1 = dt.datetime.strptime('2020/4/1  1:00:00', '%Y/%m/%d %H:%M:%S')
    t2 = dt.datetime.strptime('2020/5/1  0:00:00', '%Y/%m/%d %H:%M:%S')
    t3 = dt.datetime.strptime('2020/6/1  0:00:00', '%Y/%m/%d %H:%M:%S')
    t4 = dt.datetime.strptime('2020/7/1  0:00:00', '%Y/%m/%d %H:%M:%S')
    t5 = dt.datetime.strptime('2020/8/1  0:00:00', '%Y/%m/%d %H:%M:%S')
    t6 = dt.datetime.strptime('2020/9/1  0:00:00', '%Y/%m/%d %H:%M:%S')
    t7 = dt.datetime.strptime('2020/10/1  0:00:00', '%Y/%m/%d %H:%M:%S')
    t8 = dt.datetime.strptime('2020/11/1  0:00:00', '%Y/%m/%d %H:%M:%S')
    t9 = dt.datetime.strptime('2020/12/1  0:00:00', '%Y/%m/%d %H:%M:%S')
    t10= dt.datetime.strptime('2021/1/1  0:00:00', '%Y/%m/%d %H:%M:%S')
    t11= dt.datetime.strptime('2021/2/1  0:00:00', '%Y/%m/%d %H:%M:%S')
    t12 = dt.datetime.strptime('2021/3/1  0:00:00', '%Y/%m/%d %H:%M:%S')
    t = dt.datetime(dtime.year, dtime.month, dtime.day, dtime.hour, dtime.minute, dtime.second)

    # 月別に判定し、原単位を格納(2021/8/26 追加)
    # 4月
    if t1 <= t < t2:
        unit_price = 21.2
    # 5月
    elif t2 <= t < t3:
        unit_price = 21.6
    # 6月
    elif t3 <= t < t4:
        unit_price = 19.3
    # 7月
    elif t4 <= t < t5:
        unit_price = 19.2
    # 8月
    elif t5 <= t < t6:
        unit_price = 18.6
    # 9月
    elif t6 <= t < t7:
        unit_price = 18.0
    # 10月
    elif t7 <= t < t8:
        unit_price = 18.4
    # 11月
    elif t8 <= t < t9:
        unit_price = 18.3   
    # 12月
    elif t9 <= t < t10:
        unit_price = 16.5   
    # 1月
    elif t10 <= t < t11:
        unit_price = 16.3   
    # 2月
    elif t11 <= t < t12:
        unit_price = 16.6   
    # 3月  
    else:
        unit_price = 16.8
    
    return unit_price

#---------------------------------------------------
# 電力量,CO2排出量を1日毎で集計
#---------------------------------------------------
def electric_power_per_day_calculate(df_electric_power_per_hour):
    df_electric_temp = df_electric_power_per_hour.copy()
    df_electric_temp = df_electric_temp.set_index('DateTime')
    df_electric_per_day = pd.DataFrame()
    # 1日毎の電力量合計
    #df_electric_per_day = df_electric_temp.resample('1D').sum().copy().rename(columns={'PowerConsumption':'PowerConsumptionTotal'})
    df_electric_per_day = df_electric_temp.resample('1D').sum().copy()
    print("================df_electric_per_day================")
    print(df_electric_per_day)

    # 重回帰分析の目的関数用
    df_electric_power_for_analysis_temp = df_electric_per_day.copy()
    df_electric_power_for_analysis = df_electric_power_for_analysis_temp.reset_index()[['DateTime','PowerConsumption']].copy()
    #df_electric_power_for_analysis= df_electric_power_for_analysis_temp[['DateTime','PowerConsumption']].copy()
    print("=================df_electric_power_for_analysis==================")
    print(df_electric_power_for_analysis)
    # 1日毎の電力量平均値
    #insert_column_number = df_electric_per_day.get_loc('CO2emissionsCost') + 1
    df_electric_mean_per_day = df_electric_temp.resample('1D').mean().copy().rename(columns={'PowerConsumption':'PowerConsumptionAverage','CO2emissions':'CO2emissionsAverage','ElectricPowerCost':'ElectricPowerCostAverage','CO2emissionsCost':'CO2emissionsCostAveraege'})
    #df_electric_per_day.insert(insert_column_number,'PowerConsumptionAverage',df_electric_mean_per_day)
    df_merge_temp1 = pd.merge(df_electric_per_day,df_electric_mean_per_day,left_index=True, right_index=True)
    # # 1日毎の電力量中央値
    # df_electric_median_per_day = df_electric_temp.resample('1D').median().copy().rename(columns={'PowerConsumption':'PowerConsumptionMax','CO2emissions':'CO2emissionsMax','ElectricPowerCost':'ElectricPowerCostMax','CO2emissionsCost':'CO2emissionsCostMax'})
    # #df_electric_per_day.insert(insert_column_number+1,'PowerConsumptionMax',df_electric_max_per_day)
    # df_merge_temp2 = pd.merge(df_merge_temp1,df_electric_median_per_day,left_index=True, right_index=True)
    # 1日毎の電力量最大値
    df_electric_max_per_day = df_electric_temp.resample('1D').max().copy().rename(columns={'PowerConsumption':'PowerConsumptionMax','CO2emissions':'CO2emissionsMax','ElectricPowerCost':'ElectricPowerCostMax','CO2emissionsCost':'CO2emissionsCostMax'})
    #df_electric_per_day.insert(insert_column_number+1,'PowerConsumptionMax',df_electric_max_per_day)
    df_merge_temp2 = pd.merge(df_merge_temp1,df_electric_max_per_day,left_index=True, right_index=True)
    # 1日毎の電力量最小値
    df_electric_min_per_day = df_electric_temp.resample('1D').min().copy().rename(columns={'PowerConsumption':'PowerConsumptionMin','CO2emissions':'CO2emissionsMin','ElectricPowerCost':'ElectricPowerCostMin','CO2emissionsCost':'CO2emissionsCostMin'})
    #df_electric_per_day.insert(insert_column_number+2,'PowerConsumptionMin',df_electric_min_per_day)
    df_merge_temp3 = pd.merge(df_merge_temp2,df_electric_min_per_day,left_index=True, right_index=True)
    # 1日毎の電力量標準偏差
    df_electric_std_per_day = df_electric_temp.resample('1D').std().copy().rename(columns={'PowerConsumption':'PowerConsumptionStdev','CO2emissions':'CO2emissionsStdev','ElectricPowerCost':'ElectricPowerCostStdev','CO2emissionsCost':'CO2emissionsCostStdev'})
    #df_electric_per_day.insert(insert_column_number+3,'PowerConsumptionStdev',df_electric_std_per_day)
    df_merge_temp = pd.merge(df_merge_temp3,df_electric_std_per_day,left_index=True, right_index=True)
    df_electric_per_day = df_merge_temp.reset_index()

    return df_electric_per_day, df_electric_power_for_analysis

# 直接実行されたとき、メイン関数呼び出し
if __name__ == '__main__':
	main.main()