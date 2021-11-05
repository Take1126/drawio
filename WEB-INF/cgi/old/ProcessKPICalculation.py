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


class State():
    STOP = 1
    WAIT = 2
    RUN = 3


class Action():
    a = 1
    b = 2
    c = 3
    d = 4
    e = 5
    f = 6
    g = 7
    h = 8
    i = 9
    j = 10
    k = 11
    l = 12

#-----------------------------
# main
#-----------------------------
def main():

    # ディレクトリを指定
    power_consumption_dirname = "./Energy"
    air_consumption_dirname = "./Air"
    original_production_dirname = "./OriginalProduct"
    agg_production_dirname = "./Product"

    power_consumption_allfiles = os.listdir(power_consumption_dirname)
    air_consumption_allfiles = os.listdir(air_consumption_dirname)
    aggregated_product_allfiles = os.listdir(agg_production_dirname)
    power_file_list = []
    air_file_list = []
    aggregated_prodution_file_list = []
    # 電力消費量File数確認
    power_cons_file_count = len(power_consumption_allfiles)
    count = 0
    product_counter = power_cons_file_count
    print('=============power_cons_file_count=============')    
    print(power_cons_file_count)

    # 生産数量をProcess毎に結合
    k_line_in_product_file_path1 = original_production_dirname + "/KMA070A_Product_01612761070828297.tsv"
    k_line_in_product_file_path2 = original_production_dirname + "/KMA070B_Product_01612761070828297.tsv"
    k_line_out_product_file_path1 = original_production_dirname + "/KMA120A_Product_01612761070828297.tsv"
    k_line_out_product_file_path2 = original_production_dirname + "/KMA120B_Product_01612761070828297.tsv"
    k_line_agg_input_product_df = calc_product_aggregate(k_line_in_product_file_path1, k_line_in_product_file_path2,'KMA070')
    k_line_agg_ouput_product_df = calc_product_aggregate(k_line_out_product_file_path1, k_line_out_product_file_path2,'KMA120')
    k_line_agg_input_product_filepath = agg_production_dirname + "/KMA070_Product_01612761070828297.tsv"
    k_line_agg_ouput_product_filepath = agg_production_dirname + '/KMA120_Product_01612761070828297.tsv'
    k_line_agg_input_product_df.to_csv(k_line_agg_input_product_filepath, sep='\t', index = False, header=False,encoding="shift_jis")
    k_line_agg_ouput_product_df.to_csv(k_line_agg_ouput_product_filepath, sep='\t', index = False, header=False,encoding="shift_jis")

    m_line_in_product_file_path1 = original_production_dirname + "/MMA070A_Product_01612761070828297.tsv"
    m_line_in_product_file_path2 = original_production_dirname + "/MMA070B_Product_01612761070828297.tsv"
    m_line_out_product_file_path1 = original_production_dirname + "/MMA120A_Product_01612761070828297.tsv"
    m_line_out_product_file_path2 = original_production_dirname + "/MMA120B_Product_01612761070828297.tsv"
    m_line_agg_input_product_df = calc_product_aggregate(m_line_in_product_file_path1, m_line_in_product_file_path2,'MMA070')
    m_line_agg_ouput_product_df = calc_product_aggregate(m_line_out_product_file_path1, m_line_out_product_file_path2,'MMA120')
    m_line_agg_input_product_filepath = agg_production_dirname + "/MMA070_Product_01612761070828297.tsv"
    m_line_agg_ouput_product_filepath = agg_production_dirname + '/MMA120_Product_01612761070828297.tsv'
    m_line_agg_input_product_df.to_csv(m_line_agg_input_product_filepath, sep='\t', index = False, header=False,encoding="shift_jis")
    m_line_agg_ouput_product_df.to_csv(m_line_agg_ouput_product_filepath, sep='\t', index = False, header=False,encoding="shift_jis")

    r_line_in_product_file_path1 = original_production_dirname + "/2RMA070_Product_01612755069167877.tsv"
    #r_line_in_product_file_path2 = original_production_dirname + "/MMA070A_Product_01612761070828297.tsv"
    r_line_out_product_file_path1 = original_production_dirname + "/2RMA130A_Product_01612755069167877.tsv"
    r_line_out_product_file_path2 = original_production_dirname + "/2RMA130B_Product_01612755069167877.tsv"
    r_line_agg_input_product_df = calc_product_aggregate(r_line_in_product_file_path1, None,'2RMA070')
    r_line_agg_ouput_product_df = calc_product_aggregate(r_line_out_product_file_path1, r_line_out_product_file_path2,'2RMA130')
    r_line_agg_input_product_filepath = agg_production_dirname + "/RMA070_Product_01612755069167877.tsv"
    r_line_agg_ouput_product_filepath = agg_production_dirname + '/RMA130_Product_01612755069167877.tsv'
    r_line_agg_input_product_df.to_csv(r_line_agg_input_product_filepath, sep='\t', index = False, header=False,encoding="shift_jis")
    r_line_agg_ouput_product_df.to_csv(r_line_agg_ouput_product_filepath, sep='\t', index = False, header=False,encoding="shift_jis")

    s_line_in_product_file_path1 = original_production_dirname + "/SMA070_Product_01612755069167877.tsv"
    #s_line_in_product_file_path2 = original_production_dirname + "/MMA070A_Product_01612761070828297.tsv"
    s_line_out_product_file_path1 = original_production_dirname + "/SMA140B_Product_01612755069167877.tsv"
    s_line_out_product_file_path2 = original_production_dirname + "/SMA140C_Product_01612755069167877.tsv"
    s_line_agg_input_product_df = calc_product_aggregate(s_line_in_product_file_path1, None,'SMA070')
    s_line_agg_ouput_product_df = calc_product_aggregate(s_line_out_product_file_path1, s_line_out_product_file_path2,'SMA140')
    s_line_agg_input_product_filepath = agg_production_dirname + "/SMA070_Product_01612755069167877.tsv"
    s_line_agg_ouput_product_filepath = agg_production_dirname + '/SMA140_Product_01612755069167877.tsv'
    s_line_agg_input_product_df.to_csv(s_line_agg_input_product_filepath, sep='\t', index = False, header=False,encoding="shift_jis")
    s_line_agg_ouput_product_df.to_csv(s_line_agg_ouput_product_filepath, sep='\t', index = False, header=False,encoding="shift_jis")

    # 電力に関するKPI演算
    #merge_df = pd.DataFrame(columns = ['DateTime','ProductionProcess','PowerConsumption(Acc)','PowerConsumption','PowerCost'])
    # 電力消費量ファイルの読み込み
    product_first_calc_flag = 'False'
    product_second_calc_flag = 'False'
    product_first_calc_repeat = 'False'
    product_second_calc_repeat = 'False'
    for e in power_consumption_allfiles:
        if 'tsv' in e:
            power_full_path = os.path.join(power_consumption_dirname, e)
            power_file_list.append(power_full_path)
            #print(power_file_list)
            power_line_name = e[:-35] + '_Line'
            print('=======power_line_name==========')
            print(power_line_name)
            power_process_name = e[:-30] + '_Process'
            power_process_number = re.search(r'\d+', e)
            energy_df_minute, energy_df_hour , energy_df_day, energy_df_week, energy_df_month, energy_df_year = calc_power_consumption(power_full_path,power_process_name)
            # 圧縮空気消費量ファイルの読み込み
            for a in air_consumption_allfiles:
                if 'tsv' in a:
                    process_match_flag = None
                    air_full_path = os.path.join(air_consumption_dirname, a)
                    air_file_list.append(air_full_path)
                    air_line_name = a[-34] + '_Line'
                    print('=======air_line_name==========')
                    print(air_line_name)
                    air_process_name = a[:-30] + '_Process'
                    air_process_number = re.search(r'\d+', a) 
                    print('==air process number==')
                    print(air_process_number.group())
                    print('==power process number==')
                    print(power_process_number.group())
                    if air_line_name == power_line_name: # 2021/3/15追加　
                        if air_process_number.group() == power_process_number.group():
                            print('==process match flag==')
                            print('True')
                            process_match_flag = 'True'
                            air_df_minute, air_df_hour , air_df_day, air_df_week, air_df_month, air_df_year = calc_air_consumption(air_full_path,power_process_name,process_match_flag,energy_df_minute['DateTime'])
                        else:
                            print('==process match flag==')
                            print('False')
                            process_match_flag = 'False'
                            air_df_minute, air_df_hour , air_df_day, air_df_week, air_df_month, air_df_year = calc_air_consumption(air_full_path,power_process_name,process_match_flag,energy_df_minute['DateTime'])
                    else:
                        pass

            # 生産数量ファイルの読み込み
            for p in aggregated_product_allfiles:
                #if 'tsv' in p:
                if 'tsv' in p and product_counter > 0:
                    product_full_path = os.path.join(agg_production_dirname, p)
                    aggregated_prodution_file_list.append(product_full_path)
                    production_line_name = p[:-35] + '_Line'
                    print('=======production_line_name==========')
                    print(production_line_name)
                    production_process_name = p[:-30] + '_Process'
                    production_process_number = re.search(r'\d+', p)
                    print('================product_counter================')
                    print(product_counter)
                    print('================current_product_process_number================')
                    print(production_process_number)
                    print('================current_power_process_number================')
                    print(power_process_number)
                    if production_line_name == power_line_name: #2021/3/15修正
                        if power_process_number.group() >= '100':
                            if production_process_number.group() >= '100': 
                                product_counter = product_counter - 1
                                print('===product_counter(power_process_number.group() >= 100)===')
                                print(product_counter)
                                print('=======ProcessNo.100以上============')
                                print(e)
                                print(p)
                                print('=======product_second_calc_flag============')
                                print(product_second_calc_flag)
                                print('=======product_second_calc_repeat============')
                                print(product_second_calc_repeat)
                                if product_second_calc_flag == 'False':
                                    product_second_df_minute, product_second_df_hour , product_second_df_day, product_second_df_week, product_second_df_month, product_second_df_year = calc_product_quantity(product_full_path,production_process_name)
                                    product_df_minute = product_second_df_minute.copy()
                                    product_df_hour = product_second_df_hour.copy()
                                    product_df_day = product_second_df_day.copy()
                                    product_df_week = product_second_df_week.copy()
                                    product_df_month = product_second_df_month.copy()
                                    product_df_year = product_second_df_year.copy()
                                    #product_df = product_second_df.copy()
                                    #product_second_calc_flag = 'True'
                                else:
                                    product_df_minute = product_second_df_minute.copy()
                                    product_df_hour = product_second_df_hour.copy()
                                    product_df_day = product_second_df_day.copy()
                                    product_df_week = product_second_df_week.copy()
                                    product_df_month = product_second_df_month.copy()
                                    product_df_year = product_second_df_year.copy()                                
                                    #product_df = product_second_df.copy()
                                    product_second_calc_repeat = 'True'
                            
                                # 生産システムstate/actionの判定とDataFrame結合/階層別の生産数量、エネルギー消費量、LoadTime、UpTime集計
                                count = count + 1
                                print('=============counter=============')    
                                print(count)
                                if 'RMA' in power_process_name:
                                    power_process_name_temp = power_process_name
                                    power_process_name_replace = power_process_name_temp.replace('RMA','2RMA')
                                    output_df_minute, output_df_hour,output_df_day, output_df_week, output_df_month, output_df_year= calc_state(energy_df_minute,product_df_minute,power_process_name_replace)
                                    output_df_minute['ProductionProcess'] = power_process_name_replace
                                else:
                                    output_df_minute, output_df_hour,output_df_day, output_df_week, output_df_month, output_df_year= calc_state(energy_df_minute,product_df_minute,power_process_name)

                                # 圧縮空気消費量、CO2演算
                                output_df_minute = calc_air_co2_agg(output_df_minute, air_df_minute).copy()
                                output_df_hour = calc_air_co2_agg(output_df_hour, air_df_hour).copy()
                                output_df_day = calc_air_co2_agg(output_df_day, air_df_day).copy()
                                output_df_week = calc_air_co2_agg(output_df_week, air_df_week).copy()
                                output_df_month = calc_air_co2_agg(output_df_month, air_df_month).copy()
                                output_df_year = calc_air_co2_agg(output_df_year, air_df_year).copy()

                                out_df_minute = calc_cost_basic_unit(output_df_minute).copy()
                                out_df_hour = calc_cost_basic_unit(output_df_hour).copy()
                                out_df_day = calc_cost_basic_unit(output_df_day).copy()
                                out_df_week = calc_cost_basic_unit(output_df_week).copy()
                                out_df_month = calc_cost_basic_unit(output_df_month).copy()
                                out_df_year = calc_cost_basic_unit(output_df_year).copy()

                                # 各ProcessにおけるKPIの結合
                                if count == 1:
                                    merge_df_minute = pd.DataFrame(columns = ['DateTime','ProductionProcess','ProductionQuantity','PowerConsumption','PowerCost','PowerCostBasicUnit','CompressedAirConsumption','CompressedAirCost','CompressedAirCostBasicUnit','CO2emissions','CO2emissionsCost','CO2emissionsCostBasicUnit','LoadTime','UpTime','LoadTimeBasicUnit','UpTimeBasicUnit','state','action'])
                                    merge_df_hour = pd.DataFrame(columns = ['DateTime','ProductionProcess','ProductionQuantity','PowerConsumption','PowerCost','PowerCostBasicUnit','CompressedAirConsumption','CompressedAirCost','CompressedAirCostBasicUnit','CO2emissions','CO2emissionsCost','CO2emissionsCostBasicUnit','LoadTime','UpTime','LoadTimeBasicUnit','UpTimeBasicUnit'])
                                    merge_df_day = pd.DataFrame(columns = ['DateTime','ProductionProcess','ProductionQuantity','PowerConsumption','PowerCost','PowerCostBasicUnit','CompressedAirConsumption','CompressedAirCost','CompressedAirCostBasicUnit','CO2emissions','CO2emissionsCost','CO2emissionsCostBasicUnit','LoadTime','UpTime','LoadTimeBasicUnit','UpTimeBasicUnit'])
                                    merge_df_week = pd.DataFrame(columns = ['DateTime','ProductionProcess','ProductionQuantity','PowerConsumption','PowerCost','PowerCostBasicUnit','CompressedAirConsumption','CompressedAirCost','CompressedAirCostBasicUnit','CO2emissions','CO2emissionsCost','CO2emissionsCostBasicUnit','LoadTime','UpTime','LoadTimeBasicUnit','UpTimeBasicUnit'])
                                    merge_df_month = pd.DataFrame(columns = ['DateTime','ProductionProcess','ProductionQuantity','PowerConsumption','PowerCost','PowerCostBasicUnit','CompressedAirConsumption','CompressedAirCost','CompressedAirCostBasicUnit','CO2emissions','CO2emissionsCost','CO2emissionsCostBasicUnit','LoadTime','UpTime','LoadTimeBasicUnit','UpTimeBasicUnit'])
                                    merge_df_year = pd.DataFrame(columns = ['DateTime','ProductionProcess','ProductionQuantity','PowerConsumption','PowerCost','PowerCostBasicUnit','CompressedAirConsumption','CompressedAirCost','CompressedAirCostBasicUnit','CO2emissions','CO2emissionsCost','CO2emissionsCostBasicUnit','LoadTime','UpTime','LoadTimeBasicUnit','UpTimeBasicUnit'])
                                    merge_df_minute = merge_df_minute.append(out_df_minute)
                                    merge_df_hour = merge_df_hour.append(out_df_hour)
                                    merge_df_day = merge_df_day.append(out_df_day)
                                    merge_df_week = merge_df_week.append(out_df_week)
                                    merge_df_month = merge_df_month.append(out_df_month)
                                    merge_df_year = merge_df_year.append(out_df_year)
                                else:
                                    merge_df_minute = merge_df_minute.append(out_df_minute)
                                    merge_df_hour = merge_df_hour.append(out_df_hour)
                                    merge_df_day = merge_df_day.append(out_df_day)
                                    merge_df_week = merge_df_week.append(out_df_week)
                                    merge_df_month = merge_df_month.append(out_df_month)
                                    merge_df_year = merge_df_year.append(out_df_year)
                            else:
                                pass
                            
                        else:
                            if production_process_number.group() < '100':
                                product_counter = product_counter - 1 
                                print('===product_counter(power_process_number.group() < 100)===')
                                print(product_counter)
                                print('=======ProcessNo.90以下============')
                                print(e)
                                print(p)
                                print('=======product_first_calc_flag============')
                                print(product_first_calc_flag)
                                print('=======product_first_calc_repeat============')
                                print(product_first_calc_repeat)
                                if product_first_calc_flag == 'False':
                                    product_first_df_minute, product_first_df_hour , product_first_df_day, product_first_df_week, product_first_df_month, product_first_df_year = calc_product_quantity(product_full_path,production_process_name)
                                    product_df_minute = product_first_df_minute.copy()
                                    product_df_hour = product_first_df_hour.copy()
                                    product_df_day = product_first_df_day.copy()
                                    product_df_week = product_first_df_week.copy()
                                    product_df_month = product_first_df_month.copy()
                                    product_df_year = product_first_df_year.copy()                                
                                else:
                                    product_df_minute = product_first_df_minute.copy()
                                    product_df_hour = product_first_df_hour.copy()
                                    product_df_day = product_first_df_day.copy()
                                    product_df_week = product_first_df_week.copy()
                                    product_df_month = product_first_df_month.copy()
                                    product_df_year = product_first_df_year.copy()                                     
                                    #product_df = product_first_df.copy()
                                    product_first_calc_repeat = 'True'

                                # 生産システムstate/actionの判定とDataFrame結合/階層別の生産数量、エネルギー消費量、LoadTime、UpTime集計
                                count = count + 1
                                print('=============counter=============')    
                                print(count)
                                if 'RMA' in power_process_name:
                                    power_process_name_temp = power_process_name
                                    power_process_name_replace = power_process_name_temp.replace('RMA','2RMA')
                                    output_df_minute, output_df_hour,output_df_day, output_df_week, output_df_month, output_df_year= calc_state(energy_df_minute,product_df_minute,power_process_name_replace)
                                    output_df_minute['ProductionProcess'] = power_process_name_replace
                                else:
                                    output_df_minute, output_df_hour,output_df_day, output_df_week, output_df_month, output_df_year= calc_state(energy_df_minute,product_df_minute,power_process_name)

                                # 圧縮空気消費量、CO2演算
                                output_df_minute = calc_air_co2_agg(output_df_minute, air_df_minute).copy()
                                output_df_hour = calc_air_co2_agg(output_df_hour, air_df_hour).copy()
                                output_df_day = calc_air_co2_agg(output_df_day, air_df_day).copy()
                                output_df_week = calc_air_co2_agg(output_df_week, air_df_week).copy()
                                output_df_month = calc_air_co2_agg(output_df_month, air_df_month).copy()
                                output_df_year = calc_air_co2_agg(output_df_year, air_df_year).copy()

                                out_df_minute = calc_cost_basic_unit(output_df_minute).copy()
                                out_df_hour = calc_cost_basic_unit(output_df_hour).copy()
                                out_df_day = calc_cost_basic_unit(output_df_day).copy()
                                out_df_week = calc_cost_basic_unit(output_df_week).copy()
                                out_df_month = calc_cost_basic_unit(output_df_month).copy()
                                out_df_year = calc_cost_basic_unit(output_df_year).copy()

                                # 各ProcessにおけるKPIの結合
                                if count == 1:
                                    merge_df_minute = pd.DataFrame(columns = ['DateTime','ProductionProcess','ProductionQuantity','PowerConsumption','PowerCost','PowerCostBasicUnit','CompressedAirConsumption','CompressedAirCost','CompressedAirCostBasicUnit','CO2emissions','CO2emissionsCost','CO2emissionsCostBasicUnit','LoadTime','UpTime','LoadTimeBasicUnit','UpTimeBasicUnit','state','action'])
                                    merge_df_hour = pd.DataFrame(columns = ['DateTime','ProductionProcess','ProductionQuantity','PowerConsumption','PowerCost','PowerCostBasicUnit','CompressedAirConsumption','CompressedAirCost','CompressedAirCostBasicUnit','CO2emissions','CO2emissionsCost','CO2emissionsCostBasicUnit','LoadTime','UpTime','LoadTimeBasicUnit','UpTimeBasicUnit'])
                                    merge_df_day = pd.DataFrame(columns = ['DateTime','ProductionProcess','ProductionQuantity','PowerConsumption','PowerCost','PowerCostBasicUnit','CompressedAirConsumption','CompressedAirCost','CompressedAirCostBasicUnit','CO2emissions','CO2emissionsCost','CO2emissionsCostBasicUnit','LoadTime','UpTime','LoadTimeBasicUnit','UpTimeBasicUnit'])
                                    merge_df_week = pd.DataFrame(columns = ['DateTime','ProductionProcess','ProductionQuantity','PowerConsumption','PowerCost','PowerCostBasicUnit','CompressedAirConsumption','CompressedAirCost','CompressedAirCostBasicUnit','CO2emissions','CO2emissionsCost','CO2emissionsCostBasicUnit','LoadTime','UpTime','LoadTimeBasicUnit','UpTimeBasicUnit'])
                                    merge_df_month = pd.DataFrame(columns = ['DateTime','ProductionProcess','ProductionQuantity','PowerConsumption','PowerCost','PowerCostBasicUnit','CompressedAirConsumption','CompressedAirCost','CompressedAirCostBasicUnit','CO2emissions','CO2emissionsCost','CO2emissionsCostBasicUnit','LoadTime','UpTime','LoadTimeBasicUnit','UpTimeBasicUnit'])
                                    merge_df_year = pd.DataFrame(columns = ['DateTime','ProductionProcess','ProductionQuantity','PowerConsumption','PowerCost','PowerCostBasicUnit','CompressedAirConsumption','CompressedAirCost','CompressedAirCostBasicUnit','CO2emissions','CO2emissionsCost','CO2emissionsCostBasicUnit','LoadTime','UpTime','LoadTimeBasicUnit','UpTimeBasicUnit'])
                                    merge_df_minute = merge_df_minute.append(out_df_minute)
                                    merge_df_hour = merge_df_hour.append(out_df_hour)
                                    merge_df_day = merge_df_day.append(out_df_day)
                                    merge_df_week = merge_df_week.append(out_df_week)
                                    merge_df_month = merge_df_month.append(out_df_month)
                                    merge_df_year = merge_df_year.append(out_df_year)
                                else:
                                    merge_df_minute = merge_df_minute.append(out_df_minute)
                                    merge_df_hour = merge_df_hour.append(out_df_hour)
                                    merge_df_day = merge_df_day.append(out_df_day)
                                    merge_df_week = merge_df_week.append(out_df_week)
                                    merge_df_month = merge_df_month.append(out_df_month)
                                    merge_df_year = merge_df_year.append(out_df_year)
                            else:
                                pass
                    else:
                        pass

    merge_df_minute = merge_df_minute.sort_values(by=['DateTime','ProductionProcess'])
    merge_df_hour = merge_df_hour.sort_values(by=['DateTime','ProductionProcess'])
    merge_df_day = merge_df_day.sort_values(by=['DateTime','ProductionProcess'])
    merge_df_week = merge_df_week.sort_values(by=['DateTime','ProductionProcess'])
    merge_df_month = merge_df_month.sort_values(by=['DateTime','ProductionProcess'])
    merge_df_year = merge_df_year.sort_values(by=['DateTime','ProductionProcess'])

    # Line_KPI算出のために追加
    #print(merge_df_minute['ProductionProcess'])
    #merge_df_minute['KMA'in merge_df_minute['ProductionProcess']].groupby(['DateTime','ProductionProcess'])['ProductionQuantity','PowerConsumption','PowerCost','PowerCostBasicUnit','CompressedAirConsumption','CompressedAirCost','CompressedAirCostBasicUnit','CO2emissions','CO2emissionsCost','CO2emissionsCostBasicUnit','LoadTime','UpTime','LoadTimeBasicUnit','UpTimeBasicUnit','state','action'].sum()

    out_file_name ='Production_Process_KPI_Calculation.xlsx'
    out_file = pd.ExcelWriter(out_file_name)

    merge_df_minute.to_excel(out_file, "process_kpi_minute", index = False)
    merge_df_hour.to_excel(out_file, "process_kpi_hour", index = False)
    merge_df_day.to_excel(out_file, "process_kpi_day", index = False)
    merge_df_week.to_excel(out_file, "process_kpi_week", index = False)
    merge_df_month.to_excel(out_file, "process_kpi_month", index = False)
    merge_df_year.to_excel(out_file, "process_kpi_year", index = False)
    out_file.save()
    
    # Product_File
    #Product_file_path1 = dirname2 + "/KMA120A_Product_01612761070828297.tsv"
    #Product_file_path2 = dirname2 + "/KMA120B_Product_01612761070828297.tsv"
    #Product_df = calc_product_quantity(Product_file_path1, Product_file_path2)

    # PowerCompBasicUnitCalc  
    #PowerCompBasicUnit_df = calc_power_comsumption_basic_unit(Energy_df,Product_df)

    #ファイル出力
    #out_file = pd.ExcelWriter('KPI_Calculation.xlsx')
    #Energy_df.to_excel(out_file, "PowerCosumption", index = False)
    #Product_df.to_excel(out_file, "ProductionQuantity", index = False)
    #PowerCompBasicUnit_df.to_excel(out_file, "PowerCompBasicUnit", index = False)   
    #out_file.save()

    print("終了")

#-----------------------------
# 電力消費量の計算
#-----------------------------
def calc_power_consumption(file_path,process_name):

    df_minute = pd.DataFrame(columns = ['DateTime','PowerConsumption(Acc)','PowerConsumption','PowerUnitCost','PowerCost','CO2emissions','CO2emissionsCost'])
    #out_df = pd.DataFrame(columns = ['DateTime','PowerConsumption(Acc)','PowerConsumption','PowerUnitCost','PowerCost','PowerConsumption(Acc)Average','PowerConsumption(Acc)MovingAverage','PowerConsumption(Acc)IQR'])
    df = pd.read_table(file_path, encoding = 'shift-jis', names=('DateTime', 'ProductionResource', 'PowerConsumption(Acc)','PowerConsumption'))

    df['PowerConsumption(Acc)'] = df['PowerConsumption(Acc)'].diff().fillna(0)
    df['DateTime'] = pd.to_datetime(df['DateTime'])

    # 10分毎の計算
    df1 = df.set_index('DateTime')
    df_minute  = df1.resample('10min').sum().copy()
    #out_df['ProductionProcess'] = "KMA120"
    df_minute.insert(0,"ProductionProcess",process_name)
    df_minute['PowerConsumption'] = df_minute['PowerConsumption']/60 #kwh桁合わせ
    df_minute['PowerUnitCost'] = df_minute.index.map(lambda x: calc_electric_power_cost(x))
    df_minute['PowerCost'] =  df_minute['PowerConsumption'] * df_minute['PowerUnitCost'] 
    df_minute['CO2emissions'] = df_minute['PowerConsumption'] * 0.997 #CO2排出量計算
    df_minute['CO2emissionsCost'] = df_minute['CO2emissions'] * 0.0562 #CO2取引費用

    # 1時間毎の計算
    df_hour = df_minute.resample('1H').sum().copy()
    df_hour.insert(0,"ProductionProcess",process_name)
    # 1日毎の計算
    df_day  = df_minute.resample('1D',offset ='8H30min').sum().copy()
    df_day.insert(0,"ProductionProcess",process_name)
    # 1週間毎の計算
    df_week  = df_day.resample('1W', label='left', closed='left',loffset ='8H30min').sum().copy()
    df_week.insert(0,"ProductionProcess",process_name)
    # 1ヵ月毎の計算
    df_month  = df_day.resample('MS',label='left', closed='left',convention = 'start', loffset ='8H30min').sum().copy()
    df_month.insert(0,"ProductionProcess",process_name)
    # 1年毎の計算
    df_year = df_day.resample('AS',label='left', closed='left',convention = 'start', loffset ='8H30min').sum().copy()
    df_year.insert(0,"ProductionProcess",process_name)
    #mean(平均値)算出
    #out_df['PowerConsumption(Acc)Average'] = out_df['PowerConsumption'].mean()
    #out_df['PowerConsumption(Acc)MovingAverage'] = out_df['PowerConsumption'].rolling(6).mean()
    #out_df['PowerConsumption(Acc)IQR'] =out_df['PowerConsumption'].rolling(60).quantile(0.75) + 1.5 *( out_df['PowerConsumption'].rolling(60).quantile(0.75) - out_df['PowerConsumption'].rolling(60).quantile(0.25) )
    #q1 = out_df['PowerConsumption'].quantile(0.25)
    #q3 = out_df['PowerConsumption'].quantile(0.75)
    #IQR = q3 - q1
    #threshold = q3 + 1.5 * IQR

    #df2  = df1.resample('1W', label='left', closed='left',loffset ='8H30min').sum().copy()
    #df2  = df1.resample('1D',offset ='8H30min').sum().copy()
    #df3  = df2.resample('MS',label='left', closed='left',convention = 'start', loffset ='8H30min').sum().copy()
    out_df_minute = df_minute.drop(columns=['PowerUnitCost','PowerConsumption(Acc)']).copy()
    out_df_hour = df_hour.drop(columns=['PowerUnitCost','PowerConsumption(Acc)']).copy()
    out_df_day = df_day.drop(columns=['PowerUnitCost','PowerConsumption(Acc)']).copy()
    out_df_week = df_week.drop(columns=['PowerUnitCost','PowerConsumption(Acc)']).copy()
    out_df_month = df_month.drop(columns=['PowerUnitCost','PowerConsumption(Acc)']).copy()
    out_df_year = df_year.drop(columns=['PowerUnitCost','PowerConsumption(Acc)']).copy()
    out_df_minute = out_df_minute.reset_index()
    out_df_hour = out_df_hour.reset_index()
    out_df_day = out_df_day.reset_index()
    out_df_week = out_df_week.reset_index()
    out_df_month = out_df_month.reset_index()
    out_df_year = out_df_year.reset_index()
    #out_df['PowerConsumption'] = out_df['PowerConsumption']/60 #kwh桁合わせ
    #print ('===========平均値=============')
    #print(q1)
    #print(q3)
    #print(threshold)
    #print(out_df)
        
    return out_df_minute, out_df_hour , out_df_day, out_df_week, out_df_month, out_df_year

#-----------------------------
# 圧縮空気消費量の計算
#-----------------------------
def calc_air_consumption(file_path,process_name,match_flag,energy_datetime_sr):

    df_minute = pd.DataFrame(columns = ['DateTime','CompressedAirConsumption(Acc)','CompressedAirConsumption','CompressedAirUnitCost','CompressedAirCost','CO2emissions','CO2emissionsCost'])
    if match_flag == 'True':
        
        df = pd.read_table(file_path, encoding = 'shift-jis', names=('DateTime', 'ProductionResource', 'CompressedAirConsumption(Acc)','CompressedAirConsumption'))
        df['CompressedAirConsumption(Acc)'] = df['CompressedAirConsumption(Acc)'].diff().fillna(0)
        df['DateTime'] = pd.to_datetime(df['DateTime'])
        df1 = df.set_index('DateTime')
         # 10分毎の計算
        df_minute  = df1.resample('10min').sum().copy()
        df_minute.insert(0,"ProductionProcess",process_name)
        df_minute['CompressedAirConsumption'] = df_minute['CompressedAirConsumption']/60 #m^3桁合わせ
        df_minute['CompressedAirUnitCost'] = df_minute.index.map(lambda x: calc_compreseed_air_cost(x))
        df_minute['CompressedAirCost'] =  df_minute['CompressedAirConsumption'] * df_minute['CompressedAirUnitCost'] 
        df_minute['CO2emissions'] = df_minute['CompressedAirConsumption'] * 0.186 #CO2排出量計算
        df_minute['CO2emissionsCost'] = df_minute['CO2emissions'] * 0.0562 #CO2取引費用
        # 1時間毎の計算
        df_hour = df_minute.resample('1H').sum().copy()
        # 1日毎の計算
        df_day  = df_minute.resample('1D',offset ='8H30min').sum().copy()
        # 1週間毎の計算
        df_week  = df_day.resample('1W', label='left', closed='left',loffset ='8H30min').sum().copy()
        # 1ヵ月毎の計算
        df_month  = df_day.resample('MS',label='left', closed='left',convention = 'start', loffset ='8H30min').sum().copy()
        # 1年毎の計算
        df_year = df_day.resample('AS',label='left', closed='left',convention = 'start', loffset ='8H30min').sum().copy()
        
        out_df_minute = df_minute.drop(columns=['CompressedAirUnitCost','CompressedAirConsumption(Acc)']).copy()
        out_df_hour = df_hour.drop(columns=['CompressedAirUnitCost','CompressedAirConsumption(Acc)']).copy()
        out_df_day = df_day.drop(columns=['CompressedAirUnitCost','CompressedAirConsumption(Acc)']).copy()
        out_df_week = df_week.drop(columns=['CompressedAirUnitCost','CompressedAirConsumption(Acc)']).copy()
        out_df_month = df_month.drop(columns=['CompressedAirUnitCost','CompressedAirConsumption(Acc)']).copy()
        out_df_year = df_year.drop(columns=['CompressedAirUnitCost','CompressedAirConsumption(Acc)']).copy()
    
    else:
        df = pd.DataFrame(columns = ['DateTime','ProductionResource','CompressedAirConsumption(Acc)','CompressedAirConsumption','CompressedAirUnitCost','CompressedAirCost'])
        df['DateTime'] = energy_datetime_sr.copy()
        df_minute = df.set_index('DateTime')
        df_minute.insert(0,"ProductionProcess",process_name)
        df_minute['CompressedAirConsumption(Acc)'] = 0
        df_minute['CompressedAirConsumption'] = 0
        df_minute['CompressedAirUnitCost'] = 0
        df_minute['CompressedAirCost'] =  0
        df_minute['CO2emissions'] = 0 #CO2排出量計算
        df_minute['CO2emissionsCost'] = 0 #CO2取引費用
        # 1時間毎の計算
        df_hour = df_minute.resample('1H').sum().copy()
        # 1日毎の計算
        df_day  = df_minute.resample('1D',offset ='8H30min').sum().copy()
        # 1週間毎の計算
        df_week  = df_day.resample('1W', label='left', closed='left',loffset ='8H30min').sum().copy()
        # 1ヵ月毎の計算
        df_month  = df_day.resample('MS',label='left', closed='left',convention = 'start', loffset ='8H30min').sum().copy()
        # 1年毎の計算
        df_year = df_day.resample('AS',label='left', closed='left',convention = 'start', loffset ='8H30min').sum().copy()

        out_df_minute = df_minute.drop(columns=['CompressedAirUnitCost','CompressedAirConsumption(Acc)']).copy()
        out_df_hour = df_hour.drop(columns=['CompressedAirUnitCost','CompressedAirConsumption(Acc)']).copy()
        out_df_day = df_day.drop(columns=['CompressedAirUnitCost','CompressedAirConsumption(Acc)']).copy()
        out_df_week = df_week.drop(columns=['CompressedAirUnitCost','CompressedAirConsumption(Acc)']).copy()
        out_df_month = df_month.drop(columns=['CompressedAirUnitCost','CompressedAirConsumption(Acc)']).copy()
        out_df_year = df_year.drop(columns=['CompressedAirUnitCost','CompressedAirConsumption(Acc)']).copy()
        #out_df = df2.drop(columns=['CompressedAirUnitCost','CompressedAirConsumption(Acc)']).copy()
    
    out_df_minute = out_df_minute.reset_index()
    out_df_hour = out_df_hour.reset_index()
    out_df_day = out_df_day.reset_index()
    out_df_week = out_df_week.reset_index()
    out_df_month = out_df_month.reset_index()
    out_df_year = out_df_year.reset_index()
    #out_df = out_df.reset_index()
    #print(out_df)

    return out_df_minute, out_df_hour , out_df_day, out_df_week, out_df_month, out_df_year

#-----------------------------
# 生産数量をProcess毎/1min毎にResampling計算
#-----------------------------
def calc_product_aggregate(file_path1, file_path2, process_name):

    out_df = pd.DataFrame(columns = ['DateTime','ProductionQuantity'])


    if file_path2 == None:
        df1 = pd.read_table(file_path1, encoding = 'shift-jis', names=('DateTime', 'ProductionResource', 'ProductionQuantity'))
        df1['ProductionQuantity'] = df1['ProductionQuantity'].diff().fillna(0)
        df1['DateTime'] = pd.to_datetime(df1['DateTime'])
        df1 = df1.set_index('DateTime')
        # 0未満の処理
        df_mask1 = df1.mask(df1['ProductionQuantity'] < 0, 0)
        # 異常値(大きすぎる値のエラー処理)
        df_mask2 = df_mask1.mask(df_mask1['ProductionQuantity'] > 1000, 0)
        #df1.where(df1['ProductionQuantity'] < 0 , 0)
        df_merge = df_mask2['ProductionQuantity']
    else:
        df1 = pd.read_table(file_path1, encoding = 'shift-jis', names=('DateTime', 'ProductionResource', 'ProductionQuantity'))
        df2 = pd.read_table(file_path2, encoding = 'shift-jis', names=('DateTime', 'ProductionResource', 'ProductionQuantity'))
        df1['ProductionQuantity'] = df1['ProductionQuantity'].diff().fillna(0)
        df2['ProductionQuantity'] = df2['ProductionQuantity'].diff().fillna(0)

        df1['DateTime'] = pd.to_datetime(df1['DateTime'])
        df1 = df1.set_index('DateTime')
        #df1.where(df1['ProductionQuantity'] < 0 ,0)
        #df1_mask = df1.mask(df1['ProductionQuantity'] < 0, 0)
        # 0未満の処理
        df1_mask1 = df1.mask(df1['ProductionQuantity'] < 0, 0)
        # 異常値(大きすぎる値のエラー処理)
        df1_mask2 = df1_mask1.mask(df1_mask1['ProductionQuantity'] > 1000, 0)

        df2['DateTime'] = pd.to_datetime(df2['DateTime'])
        df2 = df2.set_index('DateTime')
       
        # 0未満の処理
        #df2_mask = df2.mask(df2['ProductionQuantity'] < 0, 0)
        # 0未満の処理
        df2_mask1 = df2.mask(df2['ProductionQuantity'] < 0, 0)
        # 異常値(大きすぎる値のエラー処理)
        df2_mask2 = df2_mask1.mask(df2_mask1['ProductionQuantity'] > 1000, 0)

        df_merge = df1_mask2['ProductionQuantity']+df2_mask2['ProductionQuantity']
       
    df3  = df_merge.resample('1min').sum().copy()
    out_df = df3.reset_index()
    out_df.insert(1,"ProductionProcess",process_name)

    return out_df

#-----------------------------
# 生産数量の計算
#-----------------------------
def calc_product_quantity(file_path,process_name):

    #out_df = pd.DataFrame(columns = ['DateTime','ProductionQuantity'])
    df1 = pd.read_table(file_path, encoding = 'shift-jis', names=('DateTime', 'ProductionResource', 'ProductionQuantity'))

    df1['DateTime'] = pd.to_datetime(df1['DateTime'])
    df1 = df1.set_index('DateTime')
    
    # 10分毎の計算
    df_minute  = df1.resample('10min').sum().copy()
    df_minute.insert(0,"ProductionProcess",process_name)
    #df3  = df1.resample('10min').sum().copy()
    #df3.insert(0,"ProductionProcess",process_name)

    # 1時間毎の計算
    df_hour = df_minute.resample('1H').sum().copy()
    # 1日毎の計算
    df_day  = df_minute.resample('1D',offset ='8H30min').sum().copy()
    # 1週間毎の計算
    df_week  = df_day.resample('1W', label='left', closed='left',loffset ='8H30min').sum().copy()
    # 1ヵ月毎の計算
    df_month  = df_day.resample('MS',label='left', closed='left',convention = 'start', loffset ='8H30min').sum().copy()
    # 1年毎の計算
    df_year = df_day.resample('AS',label='left', closed='left',convention = 'start', loffset ='8H30min').sum().copy()

    out_df_minute = df_minute.reset_index()
    out_df_hour = df_hour.reset_index()
    out_df_day = df_day.reset_index()
    out_df_week = df_week.reset_index()
    out_df_month = df_month.reset_index()
    out_df_year = df_year.reset_index()
    #out_df = df3.reset_index()
    #print (out_df)
        
    return out_df_minute, out_df_hour , out_df_day, out_df_week, out_df_month, out_df_year

#-----------------------------
# 生産システムState/Actionの判定
#-----------------------------
def calc_state(energy_df_minute, product_df,process_name):
    """
    電力とワークのデータから状態とアクションを計算
        input
            power_period_sr : 電力消費量データ
            product_period_sr : 生産数量データ
        return
            state_sr : ステート系列
            action_sr : アクション系列
    """
    # 出力用データ
    state_list = []
    action_list = []
    loadtime_list = []
    uptime_list = []
    #lodatime_basic_unit_list = []
    #uptime_basic_unit_list = []
    current_state = None
    #out_df = pd.DataFrame(columns = ['DateTime','PowerConsumption(Acc)','PowerConsumption','PowerUnitCost','PowerCost',])
    out_df_minute = energy_df_minute.copy()
    insert_column_number = out_df_minute.columns.get_loc('ProductionProcess') + 1
    out_df_minute.insert(loc=insert_column_number, column='ProductionQuantity', value=product_df['ProductionQuantity'])

    for datetime, power, product in zip(out_df_minute['DateTime'],out_df_minute['PowerConsumption'],out_df_minute['ProductionQuantity']):
        if current_state is None:
            if power < 0.08: #power == 0
                current_state = State.STOP
                current_action = Action.a if product == 0 else Action.b
                current_loadtime = 0
                current_uptime = 0     
            else:
                current_state = State.WAIT if product == 0 else State.RUN
                current_action = Action.g if product == 0 else Action.l
                current_loadtime = 10
                current_uptime = 0 if product == 0 else 10
            state_list.append(current_state)
            action_list.append(current_action)
            loadtime_list.append(current_loadtime)
            uptime_list.append(current_uptime)
            # lodatime_basic_unit,uptime_basic_unit計算
            #lodatime_basic_unit = current_loadtime / product if product != 0 else current_loadtime / 0.1
            #uptime_basic_unit = current_uptime / product if product != 0 else current_uptime / 0.1
            #lodatime_basic_unit_list.append(lodatime_basic_unit)
            #uptime_basic_unit_list.append(uptime_basic_unit)
        else:
            # STOP
            if current_state == State.STOP:
                if power < 0.08: #power == 0
                    next_state = State.STOP
                    next_action = Action.a if product == 0 else Action.b
                    next_loadtime = 0
                    next_uptime = 0                   
                else:
                    next_state = State.WAIT if product == 0 else State.RUN
                    next_action = Action.c if product == 0 else Action.d
                    next_loadtime = 10
                    next_uptime = 0 if product == 0 else 10              
            # WAIT
            elif current_state == State.WAIT:
                if power < 0.08: #power == 0
                    next_state = State.STOP
                    next_action = Action.e if product == 0 else Action.f
                    next_loadtime = 0
                    next_uptime = 0
                else:
                    next_state = State.WAIT if product == 0 else State.RUN
                    next_action = Action.g if product == 0 else Action.h
                    next_loadtime = 10
                    next_uptime = 0 if product == 0 else 10
            # RUN
            else:
                if power < 0.08: #power == 0
                    next_state = State.STOP
                    next_action = Action.i if product == 0 else Action.j
                    next_loadtime = 0
                    next_uptime = 0
                else:
                    next_state = State.WAIT if product == 0 else State.RUN
                    next_action = Action.k if product == 0 else Action.l
                    next_loadtime = 10
                    next_uptime = 0 if product == 0 else 10
            state_list.append(next_state)
            action_list.append(next_action)
            loadtime_list.append(next_loadtime)
            uptime_list.append(next_uptime)
            # lodatime_basic_unit,uptime_basic_unit計算
            #lodatime_basic_unit = current_loadtime / product if product != 0 else current_loadtime / 0.1
            #uptime_basic_unit = current_uptime / product if product != 0 else current_uptime / 0.1
            #lodatime_basic_unit_list.append(lodatime_basic_unit)
            #uptime_basic_unit_list.append(uptime_basic_unit)

            current_loadtime = next_loadtime
            current_uptime = next_uptime 
            current_state = next_state
    
    #state_sr  = pd.Series(data=state_list,  name='state',  index=energy_df_minute['DateTime'])
    #action_sr = pd.Series(data=action_list, name='action', index=energy_df_minute['DateTime'])
    #out_df.insert(loc=1, column='LoadTime', value=product_df['ProductionQuantity'])
    out_df_minute.insert(loc=len(out_df_minute.columns), column='LoadTime', value=loadtime_list)
    out_df_minute.insert(loc=len(out_df_minute.columns), column='UpTime', value=uptime_list)
    #out_df.insert(loc=len(out_df.columns), column='LoadTimeBasicUnit', value=lodatime_basic_unit_list)
    #out_df.insert(loc=len(out_df.columns), column='UpTimeBasicUnit', value=uptime_basic_unit_list)   
    out_df_minute.insert(loc=len(out_df_minute.columns), column='state', value=state_list)
    out_df_minute.insert(loc=len(out_df_minute.columns), column='action', value=action_list)
    #print ('===========State/Action=============')
    #print(state_sr)
    #print(action_sr)
    df1 = out_df_minute.copy()
    df1 = df1.set_index('DateTime')
    # 1時間毎の計算
    df_hour = df1.resample('1H').sum().copy()
    df_hour.insert(0,"ProductionProcess",process_name)
    # 1日毎の計算
    df_day  = df1.resample('1D',offset ='8H30min').sum().copy()
    df_day.insert(0,"ProductionProcess",process_name)
    # 1週間毎の計算
    df_week  = df_day.resample('1W', label='left', closed='left',loffset ='8H30min').sum().copy()
    df_week.insert(0,"ProductionProcess",process_name)
    # 1ヵ月毎の計算
    df_month  = df_day.resample('MS',label='left', closed='left',convention = 'start', loffset ='8H30min').sum().copy()
    df_month.insert(0,"ProductionProcess",process_name)
    # 1年毎の計算
    df_year = df_day.resample('AS',label='left', closed='left',convention = 'start', loffset ='8H30min').sum().copy()
    df_year.insert(0,"ProductionProcess",process_name)

    out_df_hour = df_hour.drop(columns=['state','action']).copy()
    out_df_day = df_day.drop(columns=['state','action']).copy()
    out_df_week = df_week.drop(columns=['state','action']).copy()
    out_df_month = df_month.drop(columns=['state','action']).copy()
    out_df_year = df_year.drop(columns=['state','action']).copy()

    out_df_hour = out_df_hour.reset_index()
    out_df_day = out_df_day.reset_index()
    out_df_week = out_df_week.reset_index()
    out_df_month = out_df_month.reset_index()
    out_df_year = out_df_year.reset_index()

    return out_df_minute, out_df_hour, out_df_day, out_df_week, out_df_month, out_df_year

#-----------------------------
# 電力費用原単位の計算
#-----------------------------
def calc_power_cost_basic_unit(energy_df_minute,product_df):

    out_df = pd.DataFrame(columns = ['DateTime','PowerCostBasicUnit'])
    basic_unit_df = pd.DataFrame(columns = ['DateTime','PowerCostBasicUnit'])
    df1 = energy_df_minute
    df2 = product_df
    #df1 = Energy_df.set_index('DateTime')
    #df2 = Product_df.set_index('DateTime')

    for idx, row in enumerate(df1.itertuples(index=False)):
        #print(idx)
        basic_unit_df.at[idx, 'DateTime'] = df1.at[idx, 'DateTime']
        if df2.at[idx, 'ProductionQuantity']  != 0:
            basic_unit_df.at[idx, 'PowerCostBasicUnit'] = df1.at[idx, 'PowerCost']/df2.at[idx, 'ProductionQuantity'] 
        else:#分母が0の場合のエラー処理(分母0.1として計算)
            basic_unit_df.at[idx, 'PowerCostBasicUnit'] = df1.at[idx, 'PowerCost']/0.1           

    #print(basic_unit_df)
    #print("***************************************")
    #out_df = basic_unit_df.reset_index()
    out_df = basic_unit_df
    #print (out_df)
    return out_df

#-----------------------------
# 圧縮空気費用原単位の計算
#-----------------------------
def calc_air_cost_basic_unit(air_df,product_df):

    out_df = pd.DataFrame(columns = ['DateTime','CompressedAirCostBasicUnit'])
    basic_unit_df = pd.DataFrame(columns = ['DateTime','CompressedAirCostBasicUnit'])
    df1 = air_df
    df2 = product_df

    for idx, row in enumerate(df1.itertuples(index=False)):
        basic_unit_df.at[idx, 'DateTime'] = df1.at[idx, 'DateTime']
        if df2.at[idx, 'ProductionQuantity']  != 0:
            #print(df1.at[idx, 'CompressedAirCost'])
            #print(df2.at[idx, 'ProductionQuantity'])
            basic_unit_df.at[idx, 'CompressedAirCostBasicUnit'] = df1.at[idx, 'CompressedAirCost']/df2.at[idx, 'ProductionQuantity'] 
        else:#分母が0の場合のエラー処理(分母0.1として計算)
            basic_unit_df.at[idx, 'CompressedAirCostBasicUnit'] = df1.at[idx, 'CompressedAirCost']/0.1           

    out_df = basic_unit_df

    return out_df

#-----------------------------
# 各種費用原単位の計算
#-----------------------------
def calc_cost_basic_unit(input_df):

    power_cost_basic_unit_list = []
    air_cost_basic_unit_list = []
    co2_cost_basic_unit_list = []
    loadtime_basic_unit_list = []
    uptime_basic_unit_list = []

    out_df = input_df.copy()

    for datetime, power,air,co2,loadtime,uptime,product in zip(input_df['DateTime'],input_df['PowerCost'],input_df['CompressedAirCost'],input_df['CO2emissionsCost'],input_df['LoadTime'],input_df['UpTime'],input_df['ProductionQuantity']):
        if product !=0:
            power_cost_basic_unit = power / product
            air_cost_basic_unit = air / product
            co2_cost_basic_unit = co2 / product
            loadtime_basic_unit = loadtime / product
            uptime_basic_unit = uptime / product
        else:#分母が0の場合のエラー処理(分母0.1として計算)
            power_cost_basic_unit = power / 0.1
            air_cost_basic_unit = air / 0.1
            co2_cost_basic_unit = co2 / 0.1
            loadtime_basic_unit = loadtime / 0.1
            uptime_basic_unit = uptime / 0.1            
        power_cost_basic_unit_list.append(power_cost_basic_unit)
        air_cost_basic_unit_list.append(air_cost_basic_unit)
        co2_cost_basic_unit_list.append(co2_cost_basic_unit)
        loadtime_basic_unit_list.append(loadtime_basic_unit)
        uptime_basic_unit_list.append(uptime_basic_unit)

    insert_column_number = out_df.columns.get_loc('PowerCost') + 1
    out_df.insert(loc=insert_column_number, column='PowerCostBasicUnit', value=power_cost_basic_unit_list)
    insert_column_number = out_df.columns.get_loc('CompressedAirCost') + 1
    out_df.insert(loc=insert_column_number, column='CompressedAirCostBasicUnit', value=air_cost_basic_unit_list)
    insert_column_number = out_df.columns.get_loc('CO2emissionsCost') + 1
    out_df.insert(loc=insert_column_number, column='CO2emissionsCostBasicUnit', value=co2_cost_basic_unit_list)   
    insert_column_number = out_df.columns.get_loc('UpTime') + 1
    out_df.insert(loc=insert_column_number, column='LoadTimeBasicUnit', value=loadtime_basic_unit_list)  
    insert_column_number = out_df.columns.get_loc('LoadTimeBasicUnit') + 1
    out_df.insert(loc=insert_column_number, column='UpTimeBasicUnit', value=uptime_basic_unit_list)  
    
    return out_df

#-----------------------------
# 圧縮空気消費量とCO2集計処理
#-----------------------------
def calc_air_co2_agg(df, air_df):
    out_df = df.copy()
    insert_column_number = out_df.columns.get_loc('PowerCost') + 1
    out_df['CO2emissions'] = out_df['CO2emissions'] + air_df['CO2emissions']  # CO2排出量計算
    out_df['CO2emissionsCost'] = out_df['CO2emissionsCost'] + air_df['CO2emissionsCost']  # CO2排出費用計算
    out_df.insert(loc=insert_column_number, column='CompressedAirConsumption', value=air_df['CompressedAirConsumption'])
    out_df.insert(loc=insert_column_number + 1 , column='CompressedAirCost', value=air_df['CompressedAirCost'])

    return out_df


#---------------------------------------------------
# 時間（DateTime）を入力として電力単価を返す
#---------------------------------------------------
def calc_electric_power_cost(dtime):
    unit_price = None
    t1 = dt.datetime.strptime('00:00:00', '%H:%M:%S')
    t2 = dt.datetime.strptime('08:00:00', '%H:%M:%S')
    t3 = dt.datetime.strptime('14:00:00', '%H:%M:%S')
    t4 = dt.datetime.strptime('17:00:00', '%H:%M:%S')
    t5 = dt.datetime.strptime('19:00:00', '%H:%M:%S')
    t6 = dt.datetime.strptime('22:00:00', '%H:%M:%S')
    t = dt.datetime(t1.year, t1.month, t1.day, dtime.hour, dtime.minute, dtime.second)

    # 深夜
    if t1 <= t < t2:
        unit_price = 0.39
    # デフォルト
    elif t2 <= t < t3:
        unit_price = 0.69
    # ピーク
    elif t3 <= t < t4:
        unit_price = 1.07
    # デフォルト
    elif t4 <= t < t5:
        unit_price = 0.69
    # ピーク
    elif t5 <= t < t6:
        unit_price = 1.07
    # デフォルト
    else:
        unit_price = 0.69
    
    return unit_price

#---------------------------------------------------
# 時間（DateTime）を入力として圧縮空気単価を返す
#---------------------------------------------------
def calc_compreseed_air_cost(dtime):
    unit_price = None
    t1 = dt.datetime.strptime('00:00:00', '%H:%M:%S')
    t2 = dt.datetime.strptime('08:00:00', '%H:%M:%S')
    t3 = dt.datetime.strptime('14:00:00', '%H:%M:%S')
    t4 = dt.datetime.strptime('17:00:00', '%H:%M:%S')
    t5 = dt.datetime.strptime('19:00:00', '%H:%M:%S')
    t6 = dt.datetime.strptime('22:00:00', '%H:%M:%S')
    t = dt.datetime(t1.year, t1.month, t1.day, dtime.hour, dtime.minute, dtime.second)

    # 深夜
    if t1 <= t < t2:
        unit_price = 0.07
    # デフォルト
    elif t2 <= t < t3:
        unit_price = 0.13
    # ピーク
    elif t3 <= t < t4:
        unit_price = 0.20
    # デフォルト
    elif t4 <= t < t5:
        unit_price = 0.13
    # ピーク
    elif t5 <= t < t6:
        unit_price = 0.20
    # デフォルト
    else:
        unit_price = 0.13
    
    return unit_price

#--------------------------------------------
# 指定フォルダ内の最新ファイルPathを取得する関数
#--------------------------------------------
def get_latest_modified_file_path(dirname):
        target = os.path.join(dirname, '*')
        files = [(f, os.path.getmtime(f)) for f in glob(target)]
        latest_modified_file_path = sorted(files, key=lambda files: files[1])[-1]
        print(latest_modified_file_path)
        #file_path = os.path.abspath(latest_modified_file_path[0])
        print(latest_modified_file_path[0])
        return latest_modified_file_path[0]
        #return file_path

# 直接実行されたとき、メイン関数呼び出し
if __name__ == '__main__':
    start = time.time()
    main()
    process_time = time.time() - start
    print("処理時間: {0}".format(process_time) + "[sec]")

#SMA140の電力消費量重複除去