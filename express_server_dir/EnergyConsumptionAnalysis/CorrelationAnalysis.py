# -*- coding: utf-8 -*-

import pandas as pd
import re
import sys
import os
import io
import openpyxl
import collections
import numpy as np
import main
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as dt
# from math import floor, ceil #2021/09/06 追加

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

#---------------------------------------------------
#   生産数量と電力量のDataFrameの結合
#---------------------------------------------------
def dataframe_join(electric_power_analysis_data,production_quantity_analysis_data):
    df_production = pd.DataFrame()
    df_electric_power = pd.DataFrame()
    df_production = production_quantity_analysis_data.copy()
    df_electric_power = electric_power_analysis_data.copy()
    df_merge = pd.merge(df_production,df_electric_power,on='DateTime')
    
    # データフレームの列名判定 2021/8/26
    dflist_columns = df_merge.columns.values 
    if 'ProductionQuantity' in dflist_columns:
        insert_column_number = df_merge.columns.get_loc('ProductionQuantity') + 1
    elif 'Shot_count' in dflist_columns:
        insert_column_number = df_merge.columns.get_loc('Shot_count') + 1
    else:
        print("There is not df_columns...?")

    # 'PowerConsumption' 'CO2emissions' 'ElectricPowerCost' 'CO2emissionsCost'のcolumnの移動
    # insert_column_number = df_merge.columns.get_loc('ProductionQuantity') + 1 #2021/8/26 コメント化
    insert_column_temp = df_merge.pop('PowerConsumption')
    df_merge.insert(insert_column_number,'PowerConsumption',insert_column_temp)
    insert_column_temp = df_merge.pop('CO2emissions')
    df_merge.insert(insert_column_number+1,'CO2emissions',insert_column_temp)
    insert_column_temp = df_merge.pop('ElectricPowerCost')
    df_merge.insert(insert_column_number+2,'ElectricPowerCost',insert_column_temp)
    insert_column_temp = df_merge.pop('CO2emissionsCost')
    df_merge.insert(insert_column_number+3,'CO2emissionsCost',insert_column_temp)
    print(df_merge)
    return df_merge

def basic_unit_calculate(df_input):
    df_input['PowerConsumptionBasicUnit'] = pd.Series()
    df_input['ElectricPowerCostBasicUnit'] = pd.Series()
    df_input['CO2emissionsBasicUnit'] = pd.Series()
    df_input['CO2emissionsCostBasicUnit'] = pd.Series()
    print("===========df_input================")
    print(df_input)

    # index_number_max = len(df_input.index)
    # original_index_list = []
    # print(df_input.index.values)
    # print(type(df_input.index.values))
    # original_index_list = df_input.index.values

    # index_list = []
    # index_number = 0
    # while index_number < index_number_max:
    #     index_list += [index_number]
    #     index_number = index_number + 1
    # print(len(original_index_list))
    # print(len(index_list))

    # # 上記Seriesをもとのdfにマージ
    # s_int = pd.Series(index_list,index=index_list)
    # s_int.name = "int"
    # df_input = pd.concat([df_input, s_int],join='inner',axis = 1)
    # df_input = df_input.set_index('int')
    # print(df_input)

    # # Datetime列のSeries作成(後述のfor文で上手くいかなかったため)
    # s_time = pd.Series(original_index_list,index=index_list)
    # s_time.name = "DateTime"
    # print(s_time)

    for index, row in df_input.iterrows():
        #データフレーム内の列名判定後に各種原単位を算出(2021/8/26 追加)
        dflist_columns = df_input.columns.values 
        if 'ProductionQuantity' in dflist_columns:
            df_input.at[index, 'PowerConsumptionBasicUnit'] = row['PowerConsumption']/row['ProductionQuantity'] if row['ProductionQuantity'] != 0 else 0
            df_input.at[index, 'ElectricPowerCostBasicUnit'] = row['ElectricPowerCost']/row['ProductionQuantity'] if row['ProductionQuantity'] != 0 else 0
            df_input.at[index, 'CO2emissionsBasicUnit'] = row['CO2emissions']/row['ProductionQuantity'] if row['ProductionQuantity'] != 0 else 0
            df_input.at[index, 'CO2emissionsCostBasicUnit'] = row['CO2emissionsCost']/row['ProductionQuantity'] if row['ProductionQuantity'] != 0 else 0 
        elif 'Shot_count' in dflist_columns:
            df_input.at[index, 'PowerConsumptionBasicUnit'] = row['PowerConsumption']/row['Shot_count'] if row['Shot_count'] != 0 else 0
            df_input.at[index, 'ElectricPowerCostBasicUnit'] = row['ElectricPowerCost']/row['Shot_count'] if row['Shot_count'] != 0 else 0
            df_input.at[index, 'CO2emissionsBasicUnit'] = row['CO2emissions']/row['Shot_count'] if row['Shot_count'] != 0 else 0
            df_input.at[index, 'CO2emissionsCostBasicUnit'] = row['CO2emissionsCost']/row['Shot_count'] if row['Shot_count'] != 0 else 0    
        else:
            print("There is not df_columns...?")

        #2021/8/26 以下4行分コメント化
        # df_input.at[index, 'PowerConsumptionBasicUnit'] = row['PowerConsumption']/row['ProductionQuantity'] if row['ProductionQuantity'] != 0 else 0 
        # df_input.at[index, 'ElectricPowerCostBasicUnit'] = row['ElectricPowerCost']/row['ProductionQuantity'] if row['ProductionQuantity'] != 0 else 0
        # df_input.at[index, 'CO2emissionsBasicUnit'] = row['CO2emissions']/row['ProductionQuantity'] if row['ProductionQuantity'] != 0 else 0
        # df_input.at[index, 'CO2emissionsCostBasicUnit'] = row['CO2emissionsCost']/row['ProductionQuantity'] if row['ProductionQuantity'] != 0 else 0 

        #if row['ProductionQuantity'] !=0:
        #    df_input.at[index, 'PowerConsumptionBasicUnit'] = row['PowerConsumption']/row['ProductionQuantity']
        #    df_input.at[index, 'ElectricPowerCostBasicUnit'] = row['ElectricPowerCost']/row['ProductionQuantity']
        #    df_input.at[index, 'CO2emissionsBasicUnit'] = row['CO2emissions']/row['ProductionQuantity']
        #    df_input.at[index, 'CO2emissionsCostBasicUnit'] = row['CO2emissionsCost']/row['ProductionQuantity']
        #else:
        #    df_input.at[index, 'PowerConsumptionBasicUnit'] = 0
        #    df_input.at[index, 'ElectricPowerCostBasicUnit'] = 0
        #    df_input.at[index, 'CO2emissionsBasicUnit'] = 0
        #    df_input.at[index, 'CO2emissionsCostBasicUnit'] = 0        

    insert_column_number = df_input.columns.get_loc('CO2emissionsCost') + 1
    insert_column_temp = df_input.pop('PowerConsumptionBasicUnit')
    df_input.insert(insert_column_number,'PowerConsumptionBasicUnit',insert_column_temp)
    insert_column_temp = df_input.pop('ElectricPowerCostBasicUnit')
    df_input.insert(insert_column_number+1,'ElectricPowerCostBasicUnit',insert_column_temp)
    insert_column_temp = df_input.pop('CO2emissionsBasicUnit')
    df_input.insert(insert_column_number+2,'CO2emissionsBasicUnit',insert_column_temp)
    insert_column_temp = df_input.pop('CO2emissionsCostBasicUnit')
    df_input.insert(insert_column_number+3,'CO2emissionsCostBasicUnit',insert_column_temp)
    df_basic_unit = df_input.copy()

    # print("==========df_basic_unit_before========")
    # print(df_basic_unit.head(50))

    # # 上記Seriesをもとのdfにマージ
    # df_basic_unit = pd.concat([df_basic_unit, s_time],join='inner',axis = 1)
    # df_basic_unit = df_basic_unit.set_index('DateTime')

    print("==========df_basic_unit_after========")
    print(df_basic_unit.head(50))
    return df_basic_unit

#-----------------------------------------------
# 生産システムState/Actionの判定 ( 2021/9/1 追加 )
#-----------------------------------------------
def calc_state(df_input):
    """
    電力・生産数量データから状態とアクション、段取り状態を計算
        input
            df_input : 電力消費量・生産数量+原単位データ
        return
            df_output : 電力消費量・生産数量+原単位+State/Action+段取り位置/段取り状態データ
    """
    # 出力用データ格納リスト(→1つずつ格納していき、最後は挿入してデータフレーム化)
    state_list = []
    action_list = []
    loadtime_list = []
    uptime_list = []
    laborcost_list = []
    #lodatime_basic_unit_list = []
    #uptime_basic_unit_list = []

    out_df = df_input.copy() 
    
    # State/Action判定
    current_state = None # 初期状態は初期のif文内に入るようにする
    for datetime, power, product ,moldtype in zip(out_df['DateTime'],out_df['PowerConsumption'],out_df['Shot_count'],out_df['Mold_type']): #zip：複数のリスト内要素をまとめて取り出すのに使用
        if current_state is None:
            if power < 0.08: #power == 0
                current_state = State.STOP                                   #「電力消費量=0」→1
                current_action = Action.a if product == 0 else Action.b      #「電力消費量=0 & 生産数量=0」→1、「電力消費量=0 & 生産数量>0」→2
                current_loadtime = 0
                current_uptime = 0     
            else:
                current_state = State.WAIT if product == 0 else State.RUN    #「電力消費量>0 & 生産数量=0」→2、「電力消費量>0 & 生産数量>0」→3
                current_action = Action.g if product == 0 else Action.l      #「電力消費量=0 & 生産数量=0」→1、「電力消費量=0 & 生産数量>0」→2
                current_loadtime = 10
                current_uptime = 0 if product == 0 else 10
            state_list.append(current_state)                                 #current_stateの値をリストに加える
            action_list.append(current_action)
            loadtime_list.append(current_loadtime)
            uptime_list.append(current_uptime)

            # labor_unit_cost, overtime_flag = calc_labor_cost(datetime,process_name)

            # if overtime_flag ==1: #平日残業,休日出勤
            #     if current_uptime >0:
            #         laborcost = 10 * labor_unit_cost
            #     else:
            #         laborcost =0
            # else:
            #         laborcost = 10 * labor_unit_cost
            # laborcost_list.append(laborcost)
        else:
            # STOP
            if current_state == State.STOP:                                  #前がSTOP(停止)だった場合
                if power < 0.08: #power == 0                               
                    next_state = State.STOP                                  #「電力消費量=0」→1
                    next_action = Action.a if product == 0 else Action.b     #「電力消費量=0 & 生産数量=0」→1(停止)、「電力消費量=0 & 生産数量>0」→2(異常、停止と同じ)
                    next_loadtime = 0
                    next_uptime = 0                   
                else:
                    next_state = State.WAIT if product == 0 else State.RUN   #「電力消費量>0 & 生産数量=0」→2、「電力消費量>0 & 生産数量>0」→3
                    next_action = Action.c if product == 0 else Action.d     #「電力消費量>0 & 生産数量=0」→3(立上げ)、「電力消費量>0 & 生産数量>0」→4(立上げ同時生産)
                    next_loadtime = 10
                    next_uptime = 0 if product == 0 else 10              
            # WAIT
            elif current_state == State.WAIT:                                #前がWEIT(待機)だった場合
                if power < 0.08: #power == 0
                    next_state = State.STOP                                  #「電力消費量=0」→1
                    next_action = Action.e if product == 0 else Action.f     #「電力消費量=0 & 生産数量=0」→5(立下げ)、「電力消費量=0 & 生産数量>0」→6(異常、立下げと同じ)
                    next_loadtime = 0
                    next_uptime = 0
                else:                                                        
                    next_state = State.WAIT if product == 0 else State.RUN   #「電力消費量>0 & 生産数量=0」→2、「電力消費量>0 & 生産数量>0」→3
                    next_action = Action.g if product == 0 else Action.h     #「電力消費量>0 & 生産数量=0」→7(停止)、「電力消費量>0 & 生産数量>0」→8(生産開始)
                    next_loadtime = 10
                    next_uptime = 0 if product == 0 else 10
            # RUN
            else:                                                            #前がRUN(生産開始)だった場合
                if power < 0.08: #power == 0
                    next_state = State.STOP                                  #「電力消費量=0」→1
                    next_action = Action.i if product == 0 else Action.j     #「電力消費量=0 & 生産数量=0」→9(立下げ同時生産停止)、「電力消費量=0 & 生産数量>0」→10(異常、左と同じ)
                    next_loadtime = 0
                    next_uptime = 0
                else:
                    next_state = State.WAIT if product == 0 else State.RUN   #「電力消費量>0 & 生産数量=0」→2、「電力消費量>0 & 生産数量>0」→3
                    next_action = Action.k if product == 0 else Action.l     #「電力消費量>0 & 生産数量=0」→11(立下げ)、「電力消費量>0 & 生産数量>0」→12(生産開始)
                    next_loadtime = 10
                    next_uptime = 0 if product == 0 else 10
            state_list.append(next_state)                                    #next_stateの値をリストに加える
            action_list.append(next_action) 
            loadtime_list.append(next_loadtime)  
            uptime_list.append(next_uptime) 

            current_loadtime = next_loadtime                                 #値の更新
            current_uptime = next_uptime 
            current_state = next_state

            # labor_unit_cost, overtime_flag = calc_labor_cost(datetime,process_name)

            # if overtime_flag ==1: #平日残業,休日出勤
            #     if current_uptime >0:
            #         laborcost = 10 * labor_unit_cost
            #     else:
            #         laborcost =0
            # else:
            #         laborcost = 10 * labor_unit_cost
            # laborcost_list.append(laborcost)
    
    # Setup(段取り状態フラグ)作成
    setup_list = []                                    #金型を抽出して、金型が変わったフラグをリストに格納
    previous_moldtype = None                           #最初は金型比較ができないので、最初のif文でフラグ設定するための初期状態
    for datetime,moldtype in zip(out_df['DateTime'],out_df['Mold_type']):
        if previous_moldtype is None:
            current_setup = 0                          #最初は金型変化がない→フラグ0
            previous_moldtype = moldtype               #以降金型が代入されるので、次のif文はelse内へ
            setup_list.append(current_setup)           #current_setupの値をリストに加える
        else :        
            if previous_moldtype == moldtype:          #金型が1つ前の金型と同じ場合
                current_setup = 0                      #金型変化がない→フラグ0
                previous_moldtype = moldtype           #今の金型を1つ前の金型として格納
            else :
                current_setup = 1                      #金型変化がある→フラグ1
                previous_moldtype = moldtype           #今の金型を1つ前の金型として格納
            setup_list.append(current_setup)           #current_setupの値をリストに加える

    # 段取り状態フラグの直前にもフラグを立てる
    modified_setup_list = []                           #フラグの差分取って、0にならない場合フラグを立てリストに格納
    setup_count = 0                                    #最初のif文に入るためのカウント状態
    for setup in reversed(setup_list):                 #1のフラグのある場所とその後に1が立ってしまうので、リストを逆順にしてfor文を回す
        if setup_count == 0:
            modified_setup = 0                         #最初はフラグ0設定
            old_setup = setup                          #次回比較用に修正前フラグを格納
        else:
            modified_setup = abs(old_setup - setup)    #差分を取って0or1のフラグを出力(前後が同じ：0 、異なる：1)
            old_setup = setup                          #次回比較用に修正前フラグを格納
        setup_count = setup_count + 1
        modified_setup_list.append(modified_setup)     #modified_setupの値をリストに加える
    modified_setup_list.reverse()                      #リストを逆順に読み込んだので、再度反転

    #out_df.insert(loc=1, column='LoadTime', value=product_df['ProcessProductionQuantity'])
    # out_df_minute.insert(loc=len(out_df_minute.columns), column='LaborCost', value=laborcost_list)
    # out_df.insert(loc=len(out_df.columns), column='LoadTime', value=loadtime_list)
    # out_df.insert(loc=len(out_df.columns), column='UpTime', value=uptime_list)
    #out_df.insert(loc=len(out_df.columns), column='LoadTimeBasicUnit', value=lodatime_basic_unit_list)
    #out_df.insert(loc=len(out_df.columns), column='UpTimeBasicUnit', value=uptime_basic_unit_list)   
    out_df.insert(loc=len(out_df.columns), column='State', value=state_list)
    out_df.insert(loc=len(out_df.columns), column='Action', value=action_list)
    out_df.insert(loc=len(out_df.columns), column='Number_of_setup_changes', value=setup_list) #2021/9/7 追加
    out_df.insert(loc=len(out_df.columns), column='Setup_flag', value=modified_setup_list) #2021/9/7 変更
    print ('===========State/Action + Setup=============')
    print(out_df.head(25))

    return out_df

#-----------------------------------------------
# 消費電力量のロス判定 ( 2021/9/7 追加 Setup_loss⇒Setup_and_wait_lossとSetup_and_run_lossに変更)
#-----------------------------------------------
def calc_loss(df_input):
    """
    アクションや段取り状態フラグをもとに、各種ロスに該当する消費電力量を計算
        input
            df_input : 電力消費量・生産数量+原単位+State/Action+段取り位置/段取り状態データ
        return
            df_output : input+各種ロス(Startup_loss,Down_loss,Setup_and_wait_loss,Setup_and_run_loss,Run_loss)に該当する消費電力量データ
    """
    # 出力用データ格納リスト(→1つずつ格納していき、最後は挿入してデータフレーム化)
    startup_loss_list = []     #Action==3 ⇒ Action==5 or Action==6 or Action==8になる直前までの消費電力量
    down_loss_list = []        #Action==11 ⇒ Action==5 or Action==6 or Action==8になる直前までの消費電力量
    setup_and_wait_loss_list = []       #Setup_flag == 1 and Shot_count == 0 and PowerConsumption>0 の時の消費電力量
    setup_and_run_loss_list = []       #Setup_flag == 1 and run状態(action = 4,8,12)の時の消費電力量
    run_loss_list = []         #Action == 4 Action == 8 or Action == 12の時で、PowerConsumptionBasicUnit>基準値の時の消費電力量(後で値を加えるが、現在は仮で、基準値=0.04とする(正規分布として仮定した場合、端の2σあたり)⇒重回帰分析出来たら、Mold_type毎に判定し直す
    reference_value_power_b_u = 0.04

    out_df = df_input.copy() 
    
    # Startup_lossデータ作成(Action==3⇒Action==5 or Action ==6 or Action==8になる直前までの消費電力量)                            
    previous_action = 0                                              #最初は過去のActionを参照できないので、最初のif文でフラグ設定するための初期状態
    for datetime,action,power in zip(out_df['DateTime'],out_df['Action'],out_df['PowerConsumption']):
        if action == 3:
            current_startup_loss = power                             #電力量をリスト用に格納
            previous_action = action                                 #以降金型が代入されるので、次のif文はelse内へ
        elif previous_action == 3 and (action !=5 and action != 6 and action != 8):
            current_startup_loss = power                             #電力量をリスト用に格納
            # previous_action = action                               #action判定用 #下のif文に入るためには不要
        elif previous_action == 3 and (action == 5 or action == 6 or action == 8):
            current_startup_loss = 0                                 #Startupではない→電力量は0
            previous_action = action                                 #次のループでrevious_action == 3に入らないよう格納
        else:
            current_startup_loss = 0                                 #action=3でもprevious_action=3でもない→電力量は0
            previous_action = action                                 #
        startup_loss_list.append(current_startup_loss)               #current_startup_lossの値をリストに加える

    # Down_lossデータ作成(Action==11 ⇒Action==5 or Action ==6 or Action==8になる直前までの消費電力量)
    previous_action = 0                                              #最初は過去のActionを参照できないので、最初のif文でフラグ設定するための初期状態
    for datetime,action,power in zip(out_df['DateTime'],out_df['Action'],out_df['PowerConsumption']):
        if action == 11:
            current_down_loss = power                                #電力量をリスト用に格納
            previous_action = action                                 #以降金型が代入されるので、次のif文はelse内へ
        elif previous_action == 11 and (action !=5 and action != 6 and action != 8):
            current_down_loss = power                                #電力量をリスト用に格納
            # previous_action = action                               #action判定用 #下のif文に入るためには不要
        elif previous_action == 11 and (action == 5 or action == 6 or action == 8):
            current_down_loss = 0                                    #Downではない→電力量は0
            previous_action = action                                 #次のループでrevious_action == 5に入らないよう格納
        else:
            current_down_loss = 0                                    #action=3でもprevious_action=5でもない→電力量は0
            previous_action = action                                 #
        down_loss_list.append(current_down_loss)                     #current_down_lossの値をリストに加える

    # Setup_and_wait_lossデータ作成(Setup_flag == 1 and Shot_count == 0 and PowerConsumption>0 の時の消費電力量)                            
    for datetime,setup_flag,shot,power in zip(out_df['DateTime'],out_df['Setup_flag'],out_df['Shot_count'],out_df['PowerConsumption']):
        if setup_flag == 1 and shot == 0 and power >0:
            current_setup_and_wait_loss = power                               #電力量をリスト用に格納
        else:
            current_setup_and_wait_loss = 0                                 
        setup_and_wait_loss_list.append(current_setup_and_wait_loss)                   #current_startup_lossの値をリストに加える

    # Setup_and_run_lossデータ作成(Setup_flag == 1 and ruuの時の消費電力量⇒後で評価するので今はすべて0)(2021/9/27追加)                    
    for datetime,setup_flag,shot,power in zip(out_df['DateTime'],out_df['Setup_flag'],out_df['Shot_count'],out_df['PowerConsumption']):
        if setup_flag == 1 and shot == 0 and power >0:
            current_setup_and_run_loss = 0                               #後で評価
        else:
            current_setup_and_run_loss = 0                                 
        setup_and_run_loss_list.append(current_setup_and_run_loss)                   #current_startup_lossの値をリストに加える

    # run_lossデータ作成(Action == 4 or? Action == 8 or Action == 12の時で、PowerConsumptionBasicUnit>基準値(仮に基準値=0.04；正規分布として仮定した場合、端の2σあたり)の時の消費電力量とする⇒重回帰分析出来たら、Mold_type毎に判定し直す)                  
    for datetime,action,power,power_b_u in zip(out_df['DateTime'],out_df['Action'],out_df['PowerConsumption'],out_df['PowerConsumptionBasicUnit']):
        if (action == 4 or action == 8 or action == 12) and power_b_u > reference_value_power_b_u:
            current_run_loss = power                                 #電力量をリスト用に格納
        else:
            current_run_loss = 0                                 
        run_loss_list.append(current_run_loss)                       #current_run_lossの値をリストに加える

    out_df.insert(loc=len(out_df.columns), column='Startup_loss', value=startup_loss_list)
    out_df.insert(loc=len(out_df.columns), column='Down_loss', value=down_loss_list)
    out_df.insert(loc=len(out_df.columns), column='Setup_and_wait_loss', value=setup_and_wait_loss_list)
    out_df.insert(loc=len(out_df.columns), column='Setup_and_run_loss', value=setup_and_run_loss_list)
    out_df.insert(loc=len(out_df.columns), column='Run_loss', value=run_loss_list)
    print ('===========power loss=============')
    print(out_df.head(25))

    return out_df

#-----------------------------------------------
# 消費電力量のロス計測時間判定 ( 2021/9/7 追加 )
#-----------------------------------------------
def data_per_hour_add_loss_time(df_power_loss_per_hour):
    """
    各種ロスに該当する消費電力量が計算されている行にフラグを立てることで、ロスしている時間(各行1時間)の出力
        input
            df_power_loss_per_hour : 各種ロス(Startup_loss,Down_loss,Setup_and_wait_loss,Setup_and_run_loss,Run_loss)に該当する消費電力量データを含むdf
        return
            df_loss_time_per_hour : input+各種ロス時間(Startup_loss_time,Down_loss_time,Setup_and_wait_loss_time,,Setup_and_run_loss_timeRun_loss_time)に該当する時間データ
    """
    startup_loss_time_list = []
    down_loss_time_list = []
    setup_and_wait_loss_time_list = []
    setup_and_run_loss_time_list = []
    run_loss_time_list = []
    for datetime,startup,down,setupwait,setuprun,run in zip(df_power_loss_per_hour['DateTime'],df_power_loss_per_hour['Startup_loss'],df_power_loss_per_hour['Down_loss'],df_power_loss_per_hour['Setup_and_wait_loss'],df_power_loss_per_hour['Setup_and_run_loss'],df_power_loss_per_hour['Run_loss']):
        #startup
        if startup != 0:
            current_startup_loss_time = 1
        else:
            current_startup_loss_time = 0
        startup_loss_time_list.append(current_startup_loss_time)     #current_startup_loss_timeの値をリストに加える
        #down
        if down != 0:
            current_down_loss_time = 1
        else:
            current_down_loss_time = 0
        down_loss_time_list.append(current_down_loss_time)     #current_down_loss_timeの値をリストに加える
        #setup_and_wait_loss
        if setupwait != 0:
            current_setup_and_wait_loss_time = 1
        else:
            current_setup_and_wait_loss_time = 0
        setup_and_wait_loss_time_list.append(current_setup_and_wait_loss_time)     #current_setup_and_wait_loss_timeの値をリストに加える
        #setup_and_run_loss
        if setuprun != 0:
            current_setup_and_run_loss_time = 1
        else:
            current_setup_and_run_loss_time = 0
        setup_and_run_loss_time_list.append(current_setup_and_run_loss_time)     #current_setup_and_run_loss_timeの値をリストに加える
        #run
        if run != 0:
            current_run_loss_time = 1
        else:
            current_run_loss_time = 0
        run_loss_time_list.append(current_run_loss_time)     #current_run_loss_timeの値をリストに加える

    df_loss_time_per_hour = df_power_loss_per_hour.copy()
    df_loss_time_per_hour.insert(loc=len(df_loss_time_per_hour.columns), column='Startup_loss_time', value=startup_loss_time_list)
    df_loss_time_per_hour.insert(loc=len(df_loss_time_per_hour.columns), column='Down_loss_time', value=down_loss_time_list)
    df_loss_time_per_hour.insert(loc=len(df_loss_time_per_hour.columns), column='Setup_and_wait_loss_time', value=setup_and_wait_loss_time_list)
    df_loss_time_per_hour.insert(loc=len(df_loss_time_per_hour.columns), column='Setup_and_run_loss_time', value=setup_and_run_loss_time_list)
    df_loss_time_per_hour.insert(loc=len(df_loss_time_per_hour.columns), column='Run_loss_time', value=run_loss_time_list)
    print ('===========power loss time=============')
    print(df_loss_time_per_hour.head(25))

    return df_loss_time_per_hour

#---------------------------------------------------
# 電力量,CO2排出量の統計量を計算
#---------------------------------------------------
def power_per_day_calculate_for_analysis(df_loss_time_per_hour):
    df_electric_temp = df_loss_time_per_hour[['DateTime','PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost']].copy()
    df_electric_temp = df_electric_temp.set_index('DateTime')
    print("================df_electric_temp================")
    print(df_electric_temp.head(50))    
    df_electric_per_day = pd.DataFrame()
    # 1日毎の電力量合計
    #df_electric_per_day = df_electric_temp.resample('1D').sum().copy().rename(columns={'PowerConsumption':'PowerConsumptionTotal'})
    df_electric_per_day = df_electric_temp.resample('1D').sum().copy()
    print("================df_electric_per_day_before================")
    print(df_electric_per_day)

    # 1日毎の電力量平均値
    #insert_column_number = df_electric_per_day.get_loc('CO2emissionsCost') + 1
    df_electric_mean_per_day = df_electric_temp.resample('1D').mean().copy().rename(columns={'PowerConsumption':'PowerConsumptionAverage','CO2emissions':'CO2emissionsAverage','ElectricPowerCost':'ElectricPowerCostAverage','CO2emissionsCost':'CO2emissionsCostAveraege'})
    #df_electric_per_day.insert(insert_column_number,'PowerConsumptionAverage',df_electric_mean_per_day)
    print("================df_electric_mean_per_day_after================")
    print(df_electric_mean_per_day)
    df_merge_temp1 = pd.merge(df_electric_per_day,df_electric_mean_per_day,left_index=True, right_index=True)
    print("================df_electric_temp================")
    print(df_merge_temp1.head(50))  
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
    print("================df_electric_per_day================")
    print(df_electric_per_day.head(50))  

    df_electric_per_day.drop(columns=['PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost'], inplace = True) 

    return df_electric_per_day #, df_electric_power_for_analysis

#---------------------------------------------------
# 電力量,CO2排出量の統計量を計算
#---------------------------------------------------
def power_per_month_calculate_for_analysis(df_loss_time_per_day):
    # df_electric_temp = df_loss_time_per_day[['DateTime','PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost']].copy()
    df_electric_temp = df_loss_time_per_day[['PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost']].copy()
    # df_electric_temp = df_electric_temp.set_index('DateTime')
    print("================df_electric_temp================")
    print(df_electric_temp.head(50))    
    # df_electric_per_day = pd.DataFrame()
    # 1日毎の電力量合計
    #df_electric_per_day = df_electric_temp.resample('1D').sum().copy().rename(columns={'PowerConsumption':'PowerConsumptionTotal'})
    df_electric_per_month = df_electric_temp.resample('1MS').sum().copy()
    print("================df_electric_per_day_before================")
    print(df_electric_per_month)

    # 1日毎の電力量平均値
    #insert_column_number = df_electric_per_day.get_loc('CO2emissionsCost') + 1
    df_electric_mean_per_month = df_electric_temp.resample('1MS').mean().copy().rename(columns={'PowerConsumption':'PowerConsumptionAverage','CO2emissions':'CO2emissionsAverage','ElectricPowerCost':'ElectricPowerCostAverage','CO2emissionsCost':'CO2emissionsCostAveraege'})
    #df_electric_per_day.insert(insert_column_number,'PowerConsumptionAverage',df_electric_mean_per_day)
    # print("================df_electric_mean_per_day_after================")
    # print(df_electric_mean_per_month)
    df_merge_temp1 = pd.merge(df_electric_per_month,df_electric_mean_per_month,left_index=True, right_index=True)
    print("================df_electric_temp================")
    print(df_merge_temp1.head(50))  
    # 1日毎の電力量最大値
    df_electric_max_per_month = df_electric_temp.resample('1MS').max().copy().rename(columns={'PowerConsumption':'PowerConsumptionMax','CO2emissions':'CO2emissionsMax','ElectricPowerCost':'ElectricPowerCostMax','CO2emissionsCost':'CO2emissionsCostMax'})
    #df_electric_per_day.insert(insert_column_number+1,'PowerConsumptionMax',df_electric_max_per_day)
    df_merge_temp2 = pd.merge(df_merge_temp1,df_electric_max_per_month,left_index=True, right_index=True)
    # 1日毎の電力量最小値
    df_electric_min_per_month = df_electric_temp.resample('1MS').min().copy().rename(columns={'PowerConsumption':'PowerConsumptionMin','CO2emissions':'CO2emissionsMin','ElectricPowerCost':'ElectricPowerCostMin','CO2emissionsCost':'CO2emissionsCostMin'})
    #df_electric_per_day.insert(insert_column_number+2,'PowerConsumptionMin',df_electric_min_per_day)
    df_merge_temp3 = pd.merge(df_merge_temp2,df_electric_min_per_month,left_index=True, right_index=True)
    # 1日毎の電力量標準偏差
    df_electric_std_per_month = df_electric_temp.resample('1MS').std().copy().rename(columns={'PowerConsumption':'PowerConsumptionStdev','CO2emissions':'CO2emissionsStdev','ElectricPowerCost':'ElectricPowerCostStdev','CO2emissionsCost':'CO2emissionsCostStdev'})
    #df_electric_per_day.insert(insert_column_number+3,'PowerConsumptionStdev',df_electric_std_per_day)
    df_merge_temp = pd.merge(df_merge_temp3,df_electric_std_per_month,left_index=True, right_index=True)
    df_electric_per_month = df_merge_temp.reset_index()
    print("================df_electric_per_month================")
    print(df_electric_per_month.head(50))  

    df_electric_per_month.drop(columns=['PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost'], inplace = True) 

    return df_electric_per_month #, df_electric_power_for_analysis

#---------------------------------------------------
# 電力量,CO2排出量の統計量を計算
#---------------------------------------------------
def power_per_year_calculate_for_analysis(df_loss_time_per_month):
    # df_electric_temp = df_loss_time_per_day[['DateTime','PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost']].copy()
    df_electric_temp = df_loss_time_per_month[['PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost']].copy()
    # df_electric_temp = df_electric_temp.set_index('DateTime')
    print("================df_electric_temp================")
    print(df_electric_temp.head(50))    
    # df_electric_per_day = pd.DataFrame()
    # 1日毎の電力量合計
    #df_electric_per_day = df_electric_temp.resample('1D').sum().copy().rename(columns={'PowerConsumption':'PowerConsumptionTotal'})
    df_electric_per_year = df_electric_temp.resample('1AS').sum().copy()
    print("================df_electric_per_day_before================")
    print(df_electric_per_year)

    # 1日毎の電力量平均値
    #insert_column_number = df_electric_per_day.get_loc('CO2emissionsCost') + 1
    df_electric_mean_per_year = df_electric_temp.resample('1AS').mean().copy().rename(columns={'PowerConsumption':'PowerConsumptionAverage','CO2emissions':'CO2emissionsAverage','ElectricPowerCost':'ElectricPowerCostAverage','CO2emissionsCost':'CO2emissionsCostAveraege'})
    #df_electric_per_day.insert(insert_column_number,'PowerConsumptionAverage',df_electric_mean_per_day)
    # print("================df_electric_mean_per_day_after================")
    # print(df_electric_mean_per_month)
    df_merge_temp1 = pd.merge(df_electric_per_year,df_electric_mean_per_year,left_index=True, right_index=True)
    print("================df_electric_temp================")
    print(df_merge_temp1.head(50))  
    # 1日毎の電力量最大値
    df_electric_max_per_year = df_electric_temp.resample('1AS').max().copy().rename(columns={'PowerConsumption':'PowerConsumptionMax','CO2emissions':'CO2emissionsMax','ElectricPowerCost':'ElectricPowerCostMax','CO2emissionsCost':'CO2emissionsCostMax'})
    #df_electric_per_day.insert(insert_column_number+1,'PowerConsumptionMax',df_electric_max_per_day)
    df_merge_temp2 = pd.merge(df_merge_temp1,df_electric_max_per_year,left_index=True, right_index=True)
    # 1日毎の電力量最小値
    df_electric_min_per_year = df_electric_temp.resample('1AS').min().copy().rename(columns={'PowerConsumption':'PowerConsumptionMin','CO2emissions':'CO2emissionsMin','ElectricPowerCost':'ElectricPowerCostMin','CO2emissionsCost':'CO2emissionsCostMin'})
    #df_electric_per_day.insert(insert_column_number+2,'PowerConsumptionMin',df_electric_min_per_day)
    df_merge_temp3 = pd.merge(df_merge_temp2,df_electric_min_per_year,left_index=True, right_index=True)
    # 1日毎の電力量標準偏差
    df_electric_std_per_year = df_electric_temp.resample('1AS').std().copy().rename(columns={'PowerConsumption':'PowerConsumptionStdev','CO2emissions':'CO2emissionsStdev','ElectricPowerCost':'ElectricPowerCostStdev','CO2emissionsCost':'CO2emissionsCostStdev'})
    #df_electric_per_day.insert(insert_column_number+3,'PowerConsumptionStdev',df_electric_std_per_day)
    df_merge_temp = pd.merge(df_merge_temp3,df_electric_std_per_year,left_index=True, right_index=True)
    df_electric_per_year = df_merge_temp.reset_index()
    print("================df_electric_per_year================")
    print(df_electric_per_year.head(50))  

    df_electric_per_year.drop(columns=['PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost'], inplace = True) 

    return df_electric_per_year #, df_electric_power_for_analysis

#---------------------------------------------------
#   段取り時間(1h)計算(2021/9/6 追加)
#---------------------------------------------------
def setup_time_calculate(df_setup_state_pre_resample,df_not_yet_resumple):
    #Setup_time
    df_state_action_setup = df_setup_state_pre_resample[['State','Action','Setup_flag']].copy()
    print("===========df_state_action_setup=========")   	
    print(df_state_action_setup)

    df_merge_state_action_setup = pd.merge(df_not_yet_resumple, df_state_action_setup, on='DateTime', how="outer") #リサンプルしていないdfと結合
    df_merge_nan_to_2 = df_merge_state_action_setup.fillna({'Setup_flag':2.0}) #NaNを2に変更
    # df_merge_state_action_setup_nan_to_2.replace({'Setup':{'NaN', 2.0}}, regex=True)
    print("===========df_merge_nan_to_2=========")   	
    print(df_merge_nan_to_2.head(50))

    # Datetime列のSeries作成(後述のfor文で上手くいかなかったため)
    s_time = pd.Series(df_merge_nan_to_2.index.values,index=df_merge_nan_to_2.index.values)
    s_time.name = "DateTime"
    print(s_time)

    # 上記Seriesをもとのdfにマージ
    df_time = pd.concat([df_merge_nan_to_2, s_time],join='inner',axis = 1)
    print(df_time)

    # Setup_time(段取り状態時間カウンタ)作成
    setup_time_list = []                                    #現在分数を抽出してリストに格納
    for datetime,setup in zip(df_time['DateTime'],df_merge_nan_to_2['Setup_flag']):
        if setup != 2.0:
            current_setup_time = 0                          #setup=2以外の部分→カウンタ0
            previous_minute = datetime.minute               #今の分数を1つ前の分数として格納
            # print(previous_minute)
            setup_time_list.append(current_setup_time)      #current_setup_timeの値をリストに加える
        else :        
            current_setup_time = datetime.minute - previous_minute   #カウンタに分数記録
            previous_minute = datetime.minute               #今の分数を1つ前の分数として格納
            setup_time_list.append(current_setup_time)      #current_setup_timeの値をリストに加える

    # 段取り状態時間カウンタの直後に「60-段取り状態時間カウンタの分数」計算
    modified_setup_time_list = []                          
    setup_count = 0                                    #最初のif文に入るためのカウント状態
    for setup in setup_time_list:                 
        if setup_count == 0:
            modified_setup = 0                         #最初はカウンタ0設定
            old_setup = setup                          #次回比較用に修正前カウンタを格納
        else:
            if old_setup == 0:
                modified_setup = 0                     #もともと分数が格納されていたところもカウンタ0設定
                old_setup = setup                      #次回比較用に修正前カウンタを格納
            else:
                modified_setup = 60 - old_setup        #差分を取って「60-段取り状態時間カウンタの分数」計算を出力
                old_setup = setup
        setup_count = setup_count + 1
        modified_setup_time_list.append(modified_setup)     #modified_setupの値をリストに加える

    #Setup_timeを1h→1日にリサンプルする前のdfに挿入
    df_merge_nan_to_2.insert(loc=len(df_merge_nan_to_2.columns), column='Setup_time', value=modified_setup_time_list)
    df_setup_time_not_yet_resumple = df_merge_nan_to_2.copy()
    print("===========df_setup_time_not_yet_resumple=========")   	
    print(df_setup_time_not_yet_resumple.head(50))

    #リサンプリング用にSetup_timeのみ抜き出し
    df_setup_time_resample = df_setup_time_not_yet_resumple[['Setup_time']].copy()
    print("===========df_setup_time_resample=========")   	
    print(df_setup_time_resample)

    df_out = df_setup_time_resample.copy()
    print("===========df_merge=========")   	
    print(df_out)
    return df_out

#-----------------------------------------------
# 1日単位にリサンプル(金型は不要なので出力しない) ( 2021/9/8 追加 )
#-----------------------------------------------    
def mold_drop_and_resample_per_day(df_per_day_mold_pre_merge,df_setup_time):
    setup_state_line = df_per_day_mold_pre_merge.iloc[2,1]
    #Resample前処理(数値列以外を削除)
    df_per_day_mold_pre_merge.set_index("DateTime", inplace = True)
    df_per_day_mold_pre_merge_for_drop = df_per_day_mold_pre_merge.copy()
    df_per_day_mold_pre_merge_for_drop.drop(columns=["ProductionLine","Mold_type"], inplace = True)
    df_per_day_mold_pre_merge_for_drop = pd.merge(df_per_day_mold_pre_merge_for_drop,df_setup_time,on=['DateTime'],how="left").copy()
    print("===========df_per_day_mold_pre_merge_for_drope===========")
    print(df_per_day_mold_pre_merge_for_drop)
    df_per_day_pre_merge = pd.DataFrame()
    df_per_day_pre_merge = df_per_day_mold_pre_merge_for_drop.resample("1D").sum().copy()
    df_per_day_pre_merge.insert(0,'ProductionLine', setup_state_line)
    # df_per_day_pre_merge = pd.merge(df_per_day_mold_pre_merge["ProductionLine"],df_per_day_pre_merge,on='DateTime')
    df_per_day_pre_merge.drop(columns=["State","Action","Number_of_setup_changes","Setup_flag"], inplace = True) 
    print("===========df_per_day_pre_merge===========")
    print(df_per_day_pre_merge)

    index=6
    df_per_day_pre_merge1 = df_per_day_pre_merge.iloc[0:,:index]
    df_per_day_pre_merge2 = df_per_day_pre_merge.iloc[0:,index+4:]

    return df_per_day_pre_merge1,df_per_day_pre_merge2

#-----------------------------------------------
# 1か月単位にリサンプル(金型は不要なので出力しない) ( 2021/9/14 追加 )
#-----------------------------------------------    
def mold_drop_and_resample_per_month(df_per_day_mold_pre_merge):
    line_name = df_per_day_mold_pre_merge.iloc[1,0]
    #Resample前処理(数値列以外を削除)
    # df_per_day_mold_pre_merge.set_index("DateTime", inplace = True)
    df_per_day_mold_pre_merge_for_drop = df_per_day_mold_pre_merge.copy()
    df_per_day_mold_pre_merge_for_drop.drop(columns=["ProductionLine"], inplace = True)
    print("===========df_per_day_mold_pre_merge_for_drope===========")
    print(df_per_day_mold_pre_merge_for_drop)
    df_per_month_pre_merge = pd.DataFrame()
    df_per_month_pre_merge = df_per_day_mold_pre_merge_for_drop.resample("1MS").sum().copy()
    df_per_month_pre_merge.insert(0,'ProductionLine', line_name)
    # df_per_month_pre_merge = pd.merge(df_per_day_mold_pre_merge["ProductionLine"],df_per_day_pre_merge,on='DateTime') 
    print("===========df_per_month_pre_merge===========")
    print(df_per_month_pre_merge)

    index=6
    df_per_month_pre_merge1 = df_per_month_pre_merge.iloc[0:,:index]
    print(df_per_month_pre_merge1)
    df_per_month_pre_merge2 = df_per_month_pre_merge.iloc[0:,index+4:]
    print(df_per_month_pre_merge2)
    index2=30
    df_per_month_pre_merge3 = df_per_month_pre_merge2.iloc[0:,:index2]
    print(df_per_month_pre_merge3)
    # df_per_month_pre_merge4 = df_per_month_pre_merge2.iloc[0:,index2+12:]
    # print(df_per_month_pre_merge4)
    # df_per_month_pre_merge5 = pd.merge(df_per_month_pre_merge3,df_per_month_pre_merge4,on='DateTime') 
    # print(df_per_month_pre_merge5)

    return df_per_month_pre_merge1,df_per_month_pre_merge3

#-----------------------------------------------
# 1年単位にリサンプル(金型は不要なので出力しない) ( 2021/9/14 追加 )
#-----------------------------------------------    
def mold_drop_and_resample_per_year(df_per_month_mold_pre_merge):
    line_name = df_per_month_mold_pre_merge.iloc[1,0]
    #Resample前処理(数値列以外を削除)
    # df_per_day_mold_pre_merge.set_index("DateTime", inplace = True)
    df_per_month_mold_pre_merge_for_drop = df_per_month_mold_pre_merge.copy()
    df_per_month_mold_pre_merge_for_drop.drop(columns=["ProductionLine"], inplace = True)
    # print("===========df_per_day_mold_pre_merge_for_drope===========")
    # print(df_per_day_mold_pre_merge_for_drop)
    # df_per_year_pre_merge = pd.DataFrame()
    df_per_year_pre_merge = df_per_month_mold_pre_merge_for_drop.resample("1AS").sum().copy()
    df_per_year_pre_merge.insert(0,'ProductionLine', line_name)
    # df_per_month_pre_merge = pd.merge(df_per_day_mold_pre_merge["ProductionLine"],df_per_day_pre_merge,on='DateTime') 
    print("===========df_per_month_pre_merge===========")
    print(df_per_year_pre_merge)

    index=6
    df_per_year_pre_merge1 = df_per_year_pre_merge.iloc[0:,:index]
    print(df_per_year_pre_merge1)
    df_per_year_pre_merge2 = df_per_year_pre_merge.iloc[0:,index+4:]
    print(df_per_year_pre_merge2)
    index2=30
    df_per_year_pre_merge3 = df_per_year_pre_merge2.iloc[0:,:index2]
    print(df_per_year_pre_merge3)
    # df_per_month_pre_merge4 = df_per_month_pre_merge2.iloc[0:,index2+12:]
    # print(df_per_month_pre_merge4)
    # df_per_month_pre_merge5 = pd.merge(df_per_month_pre_merge3,df_per_month_pre_merge4,on='DateTime') 
    # print(df_per_month_pre_merge5)

    return df_per_year_pre_merge1,df_per_year_pre_merge3

def mold_to_not_mold_per_day(df_per_day_basic_unit,df_per_day_pre_merge1,df_per_day_pre_merge2,df_per_day_mold_order,df_power_calculate_for_analysis_per_day):
    df_per_day_basic_unit.drop(columns=['Shot_count','PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost'], inplace = True) 
    df_per_day_merge1 = pd.merge(df_per_day_pre_merge1,df_per_day_basic_unit,on='DateTime').copy()
    df_per_day_merge2 = pd.merge(df_per_day_merge1,df_per_day_pre_merge2,on='DateTime').copy()
    df_per_day_merge3 = pd.merge(df_per_day_merge2,df_per_day_mold_order,on='DateTime').copy()
    df_per_day_merged = pd.merge(df_per_day_merge3,df_power_calculate_for_analysis_per_day,on='DateTime').copy()

    df_per_day_merged = df_per_day_merged.fillna(0) #NaNを0に変更(重回帰分析用)
    print(df_per_day_merged)

    return df_per_day_merged

def mold_to_not_mold(df_per_day_basic_unit,df_per_day_pre_merge1,df_per_day_pre_merge2,df_power_calculate_for_analysis_per_month):
    df_per_day_basic_unit.drop(columns=['Shot_count','PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost'], inplace = True) 
    df_per_day_merge1 = pd.merge(df_per_day_pre_merge1,df_per_day_basic_unit,on='DateTime').copy()
    df_per_day_merge2 = pd.merge(df_per_day_merge1,df_per_day_pre_merge2,on='DateTime').copy()
    # df_per_day_merge3 = pd.merge(df_per_day_merge2,df_per_day_mold_order,on='DateTime').copy()
    df_per_day_merged = pd.merge(df_per_day_merge2,df_power_calculate_for_analysis_per_month,on='DateTime').copy()

    df_per_day_merged = df_per_day_merged.fillna(0) #NaNを0に変更(重回帰分析用)

    return df_per_day_merged

#-----------------------------------------------
# 1日単位(Mold_typeによる分類)の集計 ( 2021/9/6 追加 )
#-----------------------------------------------    
def data_agg_per_day_mold(df_per_hour_merge,df_setup_time): 
    df_per_day1 = pd.DataFrame()
    df_per_day2 = pd.DataFrame()
    df_per_day = pd.DataFrame()
    df_per_day_temp = pd.DataFrame()
    df_electric_per_day = pd.DataFrame()
    df_per_hour_merge = pd.merge(df_per_hour_merge,df_setup_time,on=['DateTime'],how="left").copy()
    df_per_hour_merge = df_per_hour_merge.drop(len(df_per_hour_merge.index)-1) #2021/9/14 追加(分析用に最終行を削除)
    for mold_type, df_per_hour_temp in df_per_hour_merge.groupby("Mold_type"): # Mold Type毎にDataFrameを分割
        #ProductionLineの名前を抽出
        production_line_name = []
        production_line_name= df_per_hour_temp["ProductionLine"].to_list()
        production_line = production_line_name[1]
        #Resample前処理(数値列以外を削除)
        df_per_hour_temp.drop(columns=["ProductionLine","Mold_type"], inplace = True)
        df_per_hour_temp.set_index("DateTime", inplace = True)
        df_per_hour_temp1 = df_per_hour_temp.copy()
        df_per_day_merge = df_per_hour_temp1.resample("1D").sum().copy()

        df_electric_temp = df_per_hour_temp[['PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost']].copy()   
        # 1日毎の電力量合計
        df_electric_per_day = df_electric_temp.resample('1D').sum().copy()

        # 1日毎の電力量平均値
        df_electric_mean_per_day = df_electric_temp.resample('1D').mean().copy().rename(columns={'PowerConsumption':'PowerConsumptionAverage','CO2emissions':'CO2emissionsAverage','ElectricPowerCost':'ElectricPowerCostAverage','CO2emissionsCost':'CO2emissionsCostAveraege'})
        df_merge_temp1 = pd.merge(df_electric_per_day,df_electric_mean_per_day,left_index=True, right_index=True)

        # 1日毎の電力量最大値
        df_electric_max_per_day = df_electric_temp.resample('1D').max().copy().rename(columns={'PowerConsumption':'PowerConsumptionMax','CO2emissions':'CO2emissionsMax','ElectricPowerCost':'ElectricPowerCostMax','CO2emissionsCost':'CO2emissionsCostMax'})
        df_merge_temp2 = pd.merge(df_merge_temp1,df_electric_max_per_day,left_index=True, right_index=True)

        # 1日毎の電力量最小値
        df_electric_min_per_day = df_electric_temp.resample('1D').min().copy().rename(columns={'PowerConsumption':'PowerConsumptionMin','CO2emissions':'CO2emissionsMin','ElectricPowerCost':'ElectricPowerCostMin','CO2emissionsCost':'CO2emissionsCostMin'})
        df_merge_temp3 = pd.merge(df_merge_temp2,df_electric_min_per_day,left_index=True, right_index=True)

        # 1日毎の電力量標準偏差
        df_electric_std_per_day = df_electric_temp.resample('1D').std().copy().rename(columns={'PowerConsumption':'PowerConsumptionStdev','CO2emissions':'CO2emissionsStdev','ElectricPowerCost':'ElectricPowerCostStdev','CO2emissionsCost':'CO2emissionsCostStdev'})
        df_merge_temp = pd.merge(df_merge_temp3,df_electric_std_per_day,left_index=True, right_index=True)
        df_electric_per_day = df_merge_temp.reset_index()

        df_electric_per_day.drop(columns=['PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost'], inplace = True)
        df_per_day_merge2 = df_electric_per_day.copy()

        #数値が0になる行を削除
        for row in df_per_day_merge.index:
            df_per_day_temp = df_per_day1.copy()
            if (df_per_day_merge.loc[row] == 0).all():
                df_per_day_merge.drop(row, axis=0, inplace=True)
            else:
                pass
        df_per_day_merge.reset_index(inplace = True)
        df_per_day_merge.insert(1,"ProductionLine",production_line)        
        df_per_day_merge.insert(2,"Mold_type",mold_type)

        df_per_day2.reset_index(drop=True, inplace=True)
        df_per_day_merge2.insert(1,"Mold_type",mold_type)
        df_per_day1 = pd.concat([df_per_day_temp, df_per_day_merge], join='outer')
        df_per_day2 = pd.concat([df_per_day2,df_per_day_merge2], join='outer')

    df_per_day1.sort_values(by=['DateTime'], inplace = True) #DateTimeで降順sort
    # df_per_day1.drop(columns=["State","Action","Number_of_setup_changes","Setup_flag"], inplace = True) 
    df_per_day1.drop(columns=["State","Action","Setup_flag"], inplace = True) 
    df_per_day2.sort_values(by=['DateTime'], inplace = True) #DateTimeで降順sort
    df_per_day = pd.merge(df_per_day1,df_per_day2,on=['DateTime','Mold_type'],how="left").copy()

    df_per_day = df_per_day.fillna(0) #NaNを0に変更

    return df_per_day

def data_agg_per_month_mold(df_per_day_mold_merge): 
    df_per_month1 = pd.DataFrame()
    df_per_month2 = pd.DataFrame()
    df_per_month = pd.DataFrame()
    df_per_month_temp = pd.DataFrame()
    df_electric_per_month = pd.DataFrame()
    # df_per_day_merge = pd.merge(df_per_hour_merge,df_setup_time,on=['DateTime'],how="left").copy()
    print("===========df_per_day_mold_merge===========")
    print(df_per_day_mold_merge)
    for mold_type, df_per_day_temp in df_per_day_mold_merge.groupby("Mold_type"): # Mold Type毎にDataFrameを分割
        #ProductionLineの名前を抽出
        production_line_name = []
        production_line_name= df_per_day_temp["ProductionLine"].to_list()
        production_line = production_line_name[1]
        #Resample前処理(数値列以外を削除)
        df_per_day_temp.drop(columns=["ProductionLine","Mold_type"], inplace = True)
        df_per_day_temp.set_index("DateTime", inplace = True)
        df_per_day_temp1 = df_per_day_temp.copy()
        df_per_month_merge = df_per_day_temp1.resample("1MS").sum().copy()

        df_electric_temp = df_per_day_temp[['PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost']].copy()   
        # 1日毎の電力量合計
        df_electric_per_month = df_electric_temp.resample('1MS').sum().copy()

        # 1日毎の電力量平均値
        df_electric_mean_per_month = df_electric_temp.resample('1MS').mean().copy().rename(columns={'PowerConsumption':'PowerConsumptionAverage','CO2emissions':'CO2emissionsAverage','ElectricPowerCost':'ElectricPowerCostAverage','CO2emissionsCost':'CO2emissionsCostAveraege'})
        df_merge_temp1 = pd.merge(df_electric_per_month,df_electric_mean_per_month,left_index=True, right_index=True)

        # 1日毎の電力量最大値
        df_electric_max_per_month = df_electric_temp.resample('1MS').max().copy().rename(columns={'PowerConsumption':'PowerConsumptionMax','CO2emissions':'CO2emissionsMax','ElectricPowerCost':'ElectricPowerCostMax','CO2emissionsCost':'CO2emissionsCostMax'})
        df_merge_temp2 = pd.merge(df_merge_temp1,df_electric_max_per_month,left_index=True, right_index=True)

        # 1日毎の電力量最小値
        df_electric_min_per_month = df_electric_temp.resample('1MS').min().copy().rename(columns={'PowerConsumption':'PowerConsumptionMin','CO2emissions':'CO2emissionsMin','ElectricPowerCost':'ElectricPowerCostMin','CO2emissionsCost':'CO2emissionsCostMin'})
        df_merge_temp3 = pd.merge(df_merge_temp2,df_electric_min_per_month,left_index=True, right_index=True)

        # 1日毎の電力量標準偏差
        df_electric_std_per_month = df_electric_temp.resample('1MS').std().copy().rename(columns={'PowerConsumption':'PowerConsumptionStdev','CO2emissions':'CO2emissionsStdev','ElectricPowerCost':'ElectricPowerCostStdev','CO2emissionsCost':'CO2emissionsCostStdev'})
        df_merge_temp = pd.merge(df_merge_temp3,df_electric_std_per_month,left_index=True, right_index=True)
        df_electric_per_month = df_merge_temp.reset_index()

        df_electric_per_month.drop(columns=['PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost'], inplace = True)
        df_per_month_merge2 = df_electric_per_month.copy()

        #数値が0になる行を削除
        for row in df_per_month_merge.index:
            df_per_month_temp = df_per_month1.copy()
            if (df_per_month_merge.loc[row] == 0).all():
                df_per_month_merge.drop(row, axis=0, inplace=True)
            else:
                pass
        df_per_month_merge.reset_index(inplace = True)
        df_per_month_merge.insert(1,"ProductionLine",production_line)        
        df_per_month_merge.insert(2,"Mold_type",mold_type)

        df_per_month2.reset_index(drop=True, inplace=True)
        df_per_month_merge2.insert(1,"Mold_type",mold_type)
        df_per_month1 = pd.concat([df_per_month_temp, df_per_month_merge], join='outer')
        df_per_month2 = pd.concat([df_per_month2,df_per_month_merge2], join='outer')

    df_per_month1.sort_values(by=['DateTime'], inplace = True) #DateTimeで降順sort
    print("=========df_per_month1========")
    print(df_per_month1)
    df_per_month1.drop(columns=['PowerConsumptionAverage','CO2emissionsAverage','ElectricPowerCostAverage','CO2emissionsCostAveraege','PowerConsumptionMax','CO2emissionsMax','ElectricPowerCostMax','CO2emissionsCostMax','PowerConsumptionMin','CO2emissionsMin','ElectricPowerCostMin','CO2emissionsCostMin','PowerConsumptionStdev','CO2emissionsStdev','ElectricPowerCostStdev','CO2emissionsCostStdev'], inplace = True) 
    df_per_month2.sort_values(by=['DateTime'], inplace = True) #DateTimeで降順sort
    print("=========df_per_month2========")
    print(df_per_month2)
    df_per_month = pd.merge(df_per_month1,df_per_month2,on=['DateTime','Mold_type'],how="left").copy()

    df_per_month = df_per_month.fillna(0) #NaNを0に変更

    print("=========df_per_month========")
    print(df_per_month)

    return df_per_month

def data_agg_per_year_mold(df_per_month_mold_merge): 
    df_per_year1 = pd.DataFrame()
    df_per_year2 = pd.DataFrame()
    df_per_year = pd.DataFrame()
    df_per_year_temp = pd.DataFrame()
    df_electric_per_year = pd.DataFrame()
    # df_per_day_merge = pd.merge(df_per_hour_merge,df_setup_time,on=['DateTime'],how="left").copy()
    print("===========df_per_month_mold_merge===========")
    print(df_per_month_mold_merge)
    for mold_type, df_per_month_temp in df_per_month_mold_merge.groupby("Mold_type"): # Mold Type毎にDataFrameを分割
        #ProductionLineの名前を抽出
        production_line_name = []
        production_line_name= df_per_month_temp["ProductionLine"].to_list()
        production_line = production_line_name[0]
        #Resample前処理(数値列以外を削除)
        df_per_month_temp.drop(columns=["ProductionLine","Mold_type"], inplace = True)
        df_per_month_temp.set_index("DateTime", inplace = True)
        df_per_month_temp1 = df_per_month_temp.copy()
        df_per_month_temp1 = df_per_month_temp1.shift(-3,freq="M").copy()
        df_per_year_merge = df_per_month_temp1.resample("1AS").sum().copy()

        df_electric_temp = df_per_month_temp[['PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost']].copy()   
        df_electric_temp = df_electric_temp.shift(-3,freq="M").copy()
        # 1日毎の電力量合計
        df_electric_per_year = df_electric_temp.resample('1AS').sum().copy()

        # 1日毎の電力量平均値
        df_electric_mean_per_year = df_electric_temp.resample('1AS').mean().copy().rename(columns={'PowerConsumption':'PowerConsumptionAverage','CO2emissions':'CO2emissionsAverage','ElectricPowerCost':'ElectricPowerCostAverage','CO2emissionsCost':'CO2emissionsCostAveraege'})
        df_merge_temp1 = pd.merge(df_electric_per_year,df_electric_mean_per_year,left_index=True, right_index=True)

        # 1日毎の電力量最大値
        df_electric_max_per_year = df_electric_temp.resample('1AS').max().copy().rename(columns={'PowerConsumption':'PowerConsumptionMax','CO2emissions':'CO2emissionsMax','ElectricPowerCost':'ElectricPowerCostMax','CO2emissionsCost':'CO2emissionsCostMax'})
        df_merge_temp2 = pd.merge(df_merge_temp1,df_electric_max_per_year,left_index=True, right_index=True)

        # 1日毎の電力量最小値
        df_electric_min_per_year = df_electric_temp.resample('1AS').min().copy().rename(columns={'PowerConsumption':'PowerConsumptionMin','CO2emissions':'CO2emissionsMin','ElectricPowerCost':'ElectricPowerCostMin','CO2emissionsCost':'CO2emissionsCostMin'})
        df_merge_temp3 = pd.merge(df_merge_temp2,df_electric_min_per_year,left_index=True, right_index=True)

        # 1日毎の電力量標準偏差
        df_electric_std_per_year = df_electric_temp.resample('1AS').std().copy().rename(columns={'PowerConsumption':'PowerConsumptionStdev','CO2emissions':'CO2emissionsStdev','ElectricPowerCost':'ElectricPowerCostStdev','CO2emissionsCost':'CO2emissionsCostStdev'})
        df_merge_temp = pd.merge(df_merge_temp3,df_electric_std_per_year,left_index=True, right_index=True)
        df_electric_per_year = df_merge_temp.reset_index()

        df_electric_per_year.drop(columns=['PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost'], inplace = True)
        df_per_year_merge2 = df_electric_per_year.copy()

        #数値が0になる行を削除
        for row in df_per_year_merge.index:
            df_per_year_temp = df_per_year1.copy()
            if (df_per_year_merge.loc[row] == 0).all():
                df_per_year_merge.drop(row, axis=0, inplace=True)
            else:
                pass
        df_per_year_merge.reset_index(inplace = True)
        df_per_year_merge.insert(1,"ProductionLine",production_line)        
        df_per_year_merge.insert(2,"Mold_type",mold_type)

        df_per_year2.reset_index(drop=True, inplace=True)
        df_per_year_merge2.insert(1,"Mold_type",mold_type)
        df_per_year1 = pd.concat([df_per_year_temp, df_per_year_merge], join='outer')
        df_per_year2 = pd.concat([df_per_year2,df_per_year_merge2], join='outer')

    df_per_year1.sort_values(by=['DateTime'], inplace = True) #DateTimeで降順sort
    print("=========df_per_year1========")
    print(df_per_year1)
    df_per_year1.drop(columns=['PowerConsumptionAverage','CO2emissionsAverage','ElectricPowerCostAverage','CO2emissionsCostAveraege','PowerConsumptionMax','CO2emissionsMax','ElectricPowerCostMax','CO2emissionsCostMax','PowerConsumptionMin','CO2emissionsMin','ElectricPowerCostMin','CO2emissionsCostMin','PowerConsumptionStdev','CO2emissionsStdev','ElectricPowerCostStdev','CO2emissionsCostStdev'], inplace = True) 
    df_per_year2.sort_values(by=['DateTime'], inplace = True) #DateTimeで降順sort
    print("=========df_per_year2========")
    print(df_per_year2)
    df_per_year = pd.merge(df_per_year1,df_per_year2,on=['DateTime','Mold_type'],how="left").copy()

    print("=========df_per_year========")
    print(df_per_year)

    df_per_year = df_per_year.fillna(0) #NaNを0に変更

    return df_per_year

def data_processing_for_basic_unit(df_per_mold_pre_merge):
    #原単位再計算(1日毎金型毎)用データ加工
    df_per_mold_for_basic_unit = df_per_mold_pre_merge[['DateTime','Shot_count','PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost']].copy()
    df_per_mold_for_basic_unit.set_axis(pd.to_datetime(df_per_mold_for_basic_unit['DateTime']), axis='index', inplace=True)
    df_per_mold_for_basic_unit = df_per_mold_for_basic_unit.drop(columns =['DateTime'])

    index_number_max = len(df_per_mold_for_basic_unit.index)
    original_index_list = []
    print(df_per_mold_for_basic_unit.index.values)
    print(type(df_per_mold_for_basic_unit.index.values))
    original_index_list = df_per_mold_for_basic_unit.index.values

    index_list = []
    index_number = 0
    while index_number < index_number_max:
        index_list += [index_number]
        index_number = index_number + 1
    print(len(original_index_list))
    print(len(index_list))

    # 上記Seriesをもとのdfにマージ
    s_int = pd.Series(index_list,index=original_index_list)
    s_int.name = "int"
    print("==============s_int==============")    
    print(s_int)
    print("==============df_per_mold_for_basic_unit==============")
    print(df_per_mold_for_basic_unit)
    df_per_mold_for_basic_unit = pd.concat([df_per_mold_for_basic_unit, s_int],join='outer',axis = 1)
    # df_per_day_mold_for_basic_unit =pd.merge(df_per_day_mold_for_basic_unit,s_int,on='DateTime').copy()
    df_per_mold_for_basic_unit = df_per_mold_for_basic_unit.set_index('int')
    # time再挿入
    s_time = pd.Series(original_index_list ,index=original_index_list)
    s_time.name = "DateTime"
    df_per_mold_for_basic_unit.insert(loc=0, column='DateTime', value=original_index_list) 
    print("==============df_per_mold_for_basic_unit==============")
    print(df_per_mold_for_basic_unit)
    return df_per_mold_for_basic_unit,s_int,original_index_list

def merge_for_per_mold(df_per_mold_pre_merge,df_per_mold_basic_unit,s_int,original_index_list):
    df_per_mold_basic_unit.drop(columns=['Shot_count','PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost'], inplace = True) 
    print("==============df_per_mold_basic_unit==============")
    print(df_per_mold_basic_unit)
    #金型df分割(2021/9/13 関数化)
    df_per_mold_pre_merge.set_axis(pd.to_datetime(df_per_mold_pre_merge['DateTime']), axis='index', inplace=True)
    df_per_mold_pre_merge = df_per_mold_pre_merge.drop(columns =['DateTime'])
    print("==============df_per_mold_pre_merge==============")
    print(df_per_mold_pre_merge)
    index=7
    df_per_mold_pre_merge1 = df_per_mold_pre_merge.iloc[0:,:index].copy()
    print("==============df_per_mold_pre_merge1==============")
    print(df_per_mold_pre_merge1)
    df_per_mold_pre_merge1 = pd.concat([df_per_mold_pre_merge1, s_int],join='outer',axis = 1)
    df_per_mold_pre_merge1 = df_per_mold_pre_merge1.set_index('int')
    df_per_mold_pre_merge1.insert(loc=0, column='DateTime', value=original_index_list) 
    df_per_mold_pre_merge2 = df_per_mold_pre_merge.iloc[0:,index+4:].copy()
    df_per_mold_pre_merge2 = pd.concat([df_per_mold_pre_merge2, s_int],join='outer',axis = 1)
    df_per_mold_pre_merge2 = df_per_mold_pre_merge2.set_index('int')
    df_per_mold_pre_merge2.insert(loc=0, column='DateTime', value=original_index_list) 
    print("===========df_per_mold_pre_merge1===========")
    print(df_per_mold_pre_merge1)
    print("===========df_per_mold_pre_merge2===========")
    print(df_per_mold_pre_merge2)
    #dfマージ
    df_per_mold_merge1 = pd.merge(df_per_mold_pre_merge1,df_per_mold_basic_unit,on=['int','DateTime']).copy()
    print("===========df_per_mold_merge1===========")
    print(df_per_mold_merge1)
    df_per_mold = pd.merge(df_per_mold_merge1,df_per_mold_pre_merge2,on=['int','DateTime']).copy()
    # df_per_day_mold = df_per_day_mold.set_index('DateTime')
    print("===========df_per_mold===========")
    print(df_per_mold)
    # df_per_day_mold_merge3 = pd.merge(df_per_day_mold_merge2,df_per_day_mold_order,on='DateTime').copy()
    # df_per_day_merged = pd.merge(df_per_day_mold_merge3,df_power_calculate_for_analysis_per_hour,on='DateTime').copy()
    return df_per_mold

def data_agg_per_month_mold_basic_unit(df_per_day_mold_merge): 
    df_per_month1 = pd.DataFrame()
    df_per_month2 = pd.DataFrame()
    df_per_month = pd.DataFrame()
    df_per_month_temp = pd.DataFrame()
    df_electric_per_month = pd.DataFrame()
    # df_per_day_merge = pd.merge(df_per_hour_merge,df_setup_time,on=['DateTime'],how="left").copy()
    print("===========df_per_day_mold_merge===========")
    print(df_per_day_mold_merge)
    for mold_type, df_per_day_temp in df_per_day_mold_merge.groupby("Mold_type"): # Mold Type毎にDataFrameを分割
        #ProductionLineの名前を抽出
        production_line_name = []
        production_line_name= df_per_day_temp["ProductionLine"].to_list()
        production_line = production_line_name[1]
        #Resample前処理(数値列以外を削除)
        df_per_day_temp.drop(columns=["ProductionLine","Mold_type"], inplace = True)
        df_per_day_temp.set_index("DateTime", inplace = True)
        df_per_day_temp1 = df_per_day_temp.copy()
        df_per_month_merge = df_per_day_temp1.resample("1MS").sum().copy()

        df_electric_temp = df_per_day_temp[['PowerConsumptionBasicUnit','CO2emissionsBasicUnit','ElectricPowerCostBasicUnit','CO2emissionsCostBasicUnit']].copy()   
        # 1日毎の電力量合計
        df_electric_per_month = df_electric_temp.resample('1MS').sum().copy()

        # 1日毎の電力量平均値
        df_electric_mean_per_month = df_electric_temp.resample('1MS').mean().copy().rename(columns={'PowerConsumptionBasicUnit':'PowerConsumptionBasicUnitAverage','CO2emissionsBasicUnit':'CO2emissionsBasicUnitAverage','ElectricPowerCostBasicUnit':'ElectricPowerCostBasicUnitAverage','CO2emissionsCostBasicUnit':'CO2emissionsCostBasicUnitAveraege'})
        df_merge_temp1 = pd.merge(df_electric_per_month,df_electric_mean_per_month,left_index=True, right_index=True)

        # 1日毎の電力量原単位平均値
        df_electric_median_per_month = df_electric_temp.resample('1MS').median().copy().rename(columns={'PowerConsumptionBasicUnit':'PowerConsumptionBasicUnitMedian','CO2emissionsBasicUnit':'CO2emissionsBasicUnitMedian','ElectricPowerCostBasicUnit':'ElectricPowerCostBasicUnitMedian','CO2emissionsCostBasicUnit':'CO2emissionsCostBasicUnitMedian'})
        df_merge_temp2 = pd.merge(df_merge_temp1,df_electric_median_per_month,left_index=True, right_index=True)

        # 1日毎の電力量最大値
        df_electric_max_per_month = df_electric_temp.resample('1MS').max().copy().rename(columns={'PowerConsumptionBasicUnit':'PowerConsumptionBasicUnitMax','CO2emissionsBasicUnit':'CO2emissionsBasicUnitMax','ElectricPowerCostBasicUnit':'ElectricPowerCostBasicUnitMax','CO2emissionsCostBasicUnit':'CO2emissionsCostBasicUnitMax'})
        df_merge_temp3 = pd.merge(df_merge_temp2,df_electric_max_per_month,left_index=True, right_index=True)

        # 1日毎の電力量最小値
        df_electric_min_per_month = df_electric_temp.resample('1MS').min().copy().rename(columns={'PowerConsumptionBasicUnit':'PowerConsumptionBasicUnitMin','CO2emissionsBasicUnit':'CO2emissionsBasicUnitMin','ElectricPowerCostBasicUnit':'ElectricPowerCostBasicUnitMin','CO2emissionsCostBasicUnit':'CO2emissionsCostBasicUnitMin'})
        df_merge_temp4 = pd.merge(df_merge_temp3,df_electric_min_per_month,left_index=True, right_index=True)

        # 1日毎の電力量標準偏差
        df_electric_std_per_month = df_electric_temp.resample('1MS').std().copy().rename(columns={'PowerConsumptionBasicUnit':'PowerConsumptionBasicUnitStdev','CO2emissionsBasicUnit':'CO2emissionsBasicUnitStdev','ElectricPowerCostBasicUnit':'ElectricPowerCostBasicUnitStdev','CO2emissionsCostBasicUnit':'CO2emissionsCostBasicUnitStdev'})
        df_merge_temp = pd.merge(df_merge_temp4,df_electric_std_per_month,left_index=True, right_index=True)
        df_electric_per_month = df_merge_temp.reset_index()

        df_electric_per_month.drop(columns=['PowerConsumptionBasicUnit','CO2emissionsBasicUnit','ElectricPowerCostBasicUnit','CO2emissionsCostBasicUnit'], inplace = True)
        df_per_month_merge2 = df_electric_per_month.copy()

        #数値が0になる行を削除
        for row in df_per_month_merge.index:
            df_per_month_temp = df_per_month1.copy()
            if (df_per_month_merge.loc[row] == 0).all():
                df_per_month_merge.drop(row, axis=0, inplace=True)
            else:
                pass
        df_per_month_merge.reset_index(inplace = True)
        df_per_month_merge.insert(1,"ProductionLine",production_line)        
        df_per_month_merge.insert(2,"Mold_type",mold_type)

        df_per_month2.reset_index(drop=True, inplace=True)
        df_per_month_merge2.insert(1,"Mold_type",mold_type)
        df_per_month1 = pd.concat([df_per_month_temp, df_per_month_merge], join='outer')
        df_per_month2 = pd.concat([df_per_month2,df_per_month_merge2], join='outer')

    df_per_month1.sort_values(by=['DateTime'], inplace = True) #DateTimeで降順sort
    print("=========df_per_month1========")
    print(df_per_month1)
    # df_per_month1.drop(columns=['PowerConsumptionBasicUnitAverage','CO2emissionsBasicUnitAverage','ElectricPowerCostBasicUnitAverage','CO2emissionsCostBasicUnitAveraege','PowerConsumptionBasicUnitMax','CO2emissionsBasicUnitMax','ElectricPowerCostBasicUnitMax','CO2emissionsCostBasicUnitMax','PowerConsumptionBasicUnitMin','CO2emissionsBasicUnitMin','ElectricPowerCostBasicUnitMin','CO2emissionsCostBasicUnitMin','PowerConsumptionBasicUnitStdev','CO2emissionsBasicUnitStdev','ElectricPowerCostBasicUnitStdev','CO2emissionsCostBasicUnitStdev'], inplace = True) 
    df_per_month2.sort_values(by=['DateTime'], inplace = True) #DateTimeで降順sort
    print("=========df_per_month2========")
    print(df_per_month2)
    df_per_month = pd.merge(df_per_month1,df_per_month2,on=['DateTime','Mold_type'],how="left").copy()

    df_per_month3 = df_per_month[['DateTime','Mold_type']]
    print(df_per_month3)
    index2=40
    df_per_month4 = df_per_month.iloc[0:,index2:].copy()
    print(df_per_month4)

    df_per_month = pd.concat([df_per_month3,df_per_month4], axis=1).copy()
    print(df_per_month)

    df_per_month = df_per_month.fillna(0) #NaNを0に変更

    print("=========df_per_month========")
    print(df_per_month)

    return df_per_month

def data_agg_per_year_mold_basic_unit(df_per_day_mold_merge): 
    df_per_month1 = pd.DataFrame()
    df_per_month2 = pd.DataFrame()
    df_per_month = pd.DataFrame()
    df_per_month_temp = pd.DataFrame()
    df_electric_per_month = pd.DataFrame()
    # df_per_day_merge = pd.merge(df_per_hour_merge,df_setup_time,on=['DateTime'],how="left").copy()
    print("===========df_per_day_mold_merge===========")
    print(df_per_day_mold_merge)
    for mold_type, df_per_day_temp in df_per_day_mold_merge.groupby("Mold_type"): # Mold Type毎にDataFrameを分割
        #ProductionLineの名前を抽出
        production_line_name = []
        production_line_name= df_per_day_temp["ProductionLine"].to_list()
        production_line = production_line_name[0]
        #Resample前処理(数値列以外を削除)
        df_per_day_temp.drop(columns=["ProductionLine","Mold_type"], inplace = True)
        df_per_day_temp.set_index("DateTime", inplace = True)
        df_per_day_temp1 = df_per_day_temp.copy()
        df_per_day_temp1 = df_per_day_temp1.shift(-3,freq="M").copy()
        df_per_month_merge = df_per_day_temp1.resample("1AS").sum().copy()

        df_electric_temp = df_per_day_temp[['PowerConsumptionBasicUnit','CO2emissionsBasicUnit','ElectricPowerCostBasicUnit','CO2emissionsCostBasicUnit']].copy()   
        df_electric_temp = df_electric_temp.shift(-3,freq="M").copy()
        # 1日毎の電力量原単位合計
        df_electric_per_month = df_electric_temp.resample('1AS').sum().copy()

        # 1日毎の電力量原単位平均値
        df_electric_mean_per_month = df_electric_temp.resample('1AS').mean().copy().rename(columns={'PowerConsumptionBasicUnit':'PowerConsumptionBasicUnitAverage','CO2emissionsBasicUnit':'CO2emissionsBasicUnitAverage','ElectricPowerCostBasicUnit':'ElectricPowerCostBasicUnitAverage','CO2emissionsCostBasicUnit':'CO2emissionsCostBasicUnitAveraege'})
        df_merge_temp1 = pd.merge(df_electric_per_month,df_electric_mean_per_month,left_index=True, right_index=True)

        # 1日毎の電力量原単位平均値
        df_electric_median_per_month = df_electric_temp.resample('1AS').median().copy().rename(columns={'PowerConsumptionBasicUnit':'PowerConsumptionBasicUnitMedian','CO2emissionsBasicUnit':'CO2emissionsBasicUnitMedian','ElectricPowerCostBasicUnit':'ElectricPowerCostBasicUnitMedian','CO2emissionsCostBasicUnit':'CO2emissionsCostBasicUnitMedian'})
        df_merge_temp2 = pd.merge(df_merge_temp1,df_electric_median_per_month,left_index=True, right_index=True)

        # 1日毎の電力量原単位最大値
        df_electric_max_per_month = df_electric_temp.resample('1AS').max().copy().rename(columns={'PowerConsumptionBasicUnit':'PowerConsumptionBasicUnitMax','CO2emissionsBasicUnit':'CO2emissionsBasicUnitMax','ElectricPowerCostBasicUnit':'ElectricPowerCostBasicUnitMax','CO2emissionsCostBasicUnit':'CO2emissionsCostBasicUnitMax'})
        df_merge_temp3 = pd.merge(df_merge_temp2,df_electric_max_per_month,left_index=True, right_index=True)

        # 1日毎の電力量原単位最小値
        df_electric_min_per_month = df_electric_temp.resample('1AS').min().copy().rename(columns={'PowerConsumptionBasicUnit':'PowerConsumptionBasicUnitMin','CO2emissionsBasicUnit':'CO2emissionsBasicUnitMin','ElectricPowerCostBasicUnit':'ElectricPowerCostBasicUnitMin','CO2emissionsCostBasicUnit':'CO2emissionsCostBasicUnitMin'})
        df_merge_temp4 = pd.merge(df_merge_temp3,df_electric_min_per_month,left_index=True, right_index=True)

        # 1日毎の電力量原単位標準偏差
        df_electric_std_per_month = df_electric_temp.resample('1AS').std().copy().rename(columns={'PowerConsumptionBasicUnit':'PowerConsumptionBasicUnitStdev','CO2emissionsBasicUnit':'CO2emissionsBasicUnitStdev','ElectricPowerCostBasicUnit':'ElectricPowerCostBasicUnitStdev','CO2emissionsCostBasicUnit':'CO2emissionsCostBasicUnitStdev'})
        df_merge_temp = pd.merge(df_merge_temp4,df_electric_std_per_month,left_index=True, right_index=True)
        df_electric_per_month = df_merge_temp.reset_index()

        df_electric_per_month.drop(columns=['PowerConsumptionBasicUnit','CO2emissionsBasicUnit','ElectricPowerCostBasicUnit','CO2emissionsCostBasicUnit'], inplace = True)
        df_per_month_merge2 = df_electric_per_month.copy()

        #数値が0になる行を削除
        for row in df_per_month_merge.index:
            df_per_month_temp = df_per_month1.copy()
            if (df_per_month_merge.loc[row] == 0).all():
                df_per_month_merge.drop(row, axis=0, inplace=True)
            else:
                pass
        df_per_month_merge.reset_index(inplace = True)
        df_per_month_merge.insert(1,"ProductionLine",production_line)        
        df_per_month_merge.insert(2,"Mold_type",mold_type)

        df_per_month2.reset_index(drop=True, inplace=True)
        df_per_month_merge2.insert(1,"Mold_type",mold_type)
        df_per_month1 = pd.concat([df_per_month_temp, df_per_month_merge], join='outer')
        df_per_month2 = pd.concat([df_per_month2,df_per_month_merge2], join='outer')

    df_per_month1.sort_values(by=['DateTime'], inplace = True) #DateTimeで降順sort
    print("=========df_per_month1========")
    print(df_per_month1)
    # df_per_month1.drop(columns=['PowerConsumptionBasicUnitAverage','CO2emissionsBasicUnitAverage','ElectricPowerCostBasicUnitAverage','CO2emissionsCostBasicUnitAveraege','PowerConsumptionBasicUnitMax','CO2emissionsBasicUnitMax','ElectricPowerCostBasicUnitMax','CO2emissionsCostBasicUnitMax','PowerConsumptionBasicUnitMin','CO2emissionsBasicUnitMin','ElectricPowerCostBasicUnitMin','CO2emissionsCostBasicUnitMin','PowerConsumptionBasicUnitStdev','CO2emissionsBasicUnitStdev','ElectricPowerCostBasicUnitStdev','CO2emissionsCostBasicUnitStdev'], inplace = True) 
    df_per_month2.sort_values(by=['DateTime'], inplace = True) #DateTimeで降順sort
    print("=========df_per_month2========")
    print(df_per_month2)
    df_per_month = pd.merge(df_per_month1,df_per_month2,on=['DateTime','Mold_type'],how="left").copy()

    df_per_month3 = df_per_month[['DateTime','Mold_type']]
    print(df_per_month3)
    index2=40
    df_per_month4 = df_per_month.iloc[0:,index2:].copy()
    print(df_per_month4)

    df_per_month = pd.concat([df_per_month3,df_per_month4], axis=1).copy()
    print(df_per_month)

    df_per_month = df_per_month.fillna(0) #NaNを0に変更

    print("=========df_per_month========")
    print(df_per_month)

    return df_per_month

def df_per_year_mold_output_setting(df_per_year_mold):

    df_per_year_mold_output = df_per_year_mold.copy()
    df_per_year_mold_output.set_index("DateTime", inplace = True)
    df_per_year_mold_output = df_per_year_mold_output.shift(3,freq="MS").copy()
    df_per_year_mold_output.insert(loc=0, column='DateTime', value=df_per_year_mold_output.index.values) 

    return df_per_year_mold_output

#gropbyによる金型毎df作成(2021/9/21 追加)
def calculate_basicunit_per_mold(df_pre_groupby2):
    df_basicunit_mean_per_mold = df_pre_groupby2.groupby(["Mold_type"])[["PowerConsumptionBasicUnit"]].mean().copy()
    df_basicunit_mean_per_mold.columns = ["PowerConsumptionBasicUnit(Average)"]
    df_basicunit_max_per_mold = df_pre_groupby2.groupby(["Mold_type"])[["PowerConsumptionBasicUnit"]].max().copy()
    df_basicunit_max_per_mold.columns = ["PowerConsumptionBasicUnit(Max)"]
    df_basicunit_min_per_mold = df_pre_groupby2.groupby(["Mold_type"])[["PowerConsumptionBasicUnit"]].min().copy()
    df_basicunit_min_per_mold.columns = ["PowerConsumptionBasicUnit(Min)"]
    df_basicunit_stdev_per_mold = df_pre_groupby2.groupby(["Mold_type"])[["PowerConsumptionBasicUnit"]].std().copy()
    df_basicunit_stdev_per_mold.columns = ["PowerConsumptionBasicUnit(StandardDeviation)"]
    df_shotcount_sum_per_mold = df_pre_groupby2.groupby(["Mold_type"])[["Shot_count"]].sum().copy()
    df_shotcount_sum_per_mold.columns = ["Shot_count(Sum)"]
    df_datacount_per_mold = df_pre_groupby2.groupby(["Mold_type"]).size().copy()
    df_datacount_per_mold.name = "Data_count"

    df_basicunit_per_mold = pd.concat([df_basicunit_min_per_mold, df_basicunit_mean_per_mold], axis=1,join='inner')
    df_basicunit_per_mold = pd.concat([df_basicunit_per_mold, df_basicunit_max_per_mold], axis=1,join='inner')
    df_basicunit_per_mold = pd.concat([df_basicunit_per_mold, df_basicunit_stdev_per_mold], axis=1,join='inner')
    df_basicunit_per_mold = pd.concat([df_basicunit_per_mold, df_shotcount_sum_per_mold], axis=1,join='inner')
    df_basicunit_per_mold = pd.concat([df_basicunit_per_mold, df_datacount_per_mold], axis=1,join='inner').reset_index()
    return df_basicunit_per_mold

def calculate_basicunit_per_action(df_pre_groupby1):
    df_basicunit_mean_per_action = df_pre_groupby1.groupby(["Action"])[["PowerConsumptionBasicUnit"]].mean().copy()
    df_basicunit_mean_per_action.columns = ["PowerConsumptionBasicUnit(Average)"]
    df_basicunit_max_per_action = df_pre_groupby1.groupby(["Action"])[["PowerConsumptionBasicUnit"]].max().copy()
    df_basicunit_max_per_action.columns = ["PowerConsumptionBasicUnit(Max)"]
    df_basicunit_min_per_action = df_pre_groupby1.groupby(["Action"])[["PowerConsumptionBasicUnit"]].min().copy()
    df_basicunit_min_per_action.columns = ["PowerConsumptionBasicUnit(Min)"]
    df_basicunit_stdev_per_action = df_pre_groupby1.groupby(["Action"])[["PowerConsumptionBasicUnit"]].std().copy()
    df_basicunit_stdev_per_action.columns = ["PowerConsumptionBasicUnit(StandardDeviation)"]
    df_shotcount_sum_per_action = df_pre_groupby1.groupby(["Action"])[["Shot_count"]].sum().copy()
    df_shotcount_sum_per_action.columns = ["Shot_count(Sum)"]
    df_datacount_per_action = df_pre_groupby1.groupby(["Action"]).size().copy()
    df_datacount_per_action.name = "Data_count"

    df_basicunit_per_action = pd.concat([df_basicunit_min_per_action, df_basicunit_mean_per_action], axis=1,join='inner')
    df_basicunit_per_action = pd.concat([df_basicunit_per_action, df_basicunit_max_per_action], axis=1,join='inner')
    df_basicunit_per_action = pd.concat([df_basicunit_per_action, df_basicunit_stdev_per_action], axis=1,join='inner')
    df_basicunit_per_action = pd.concat([df_basicunit_per_action, df_shotcount_sum_per_action], axis=1,join='inner')
    df_basicunit_per_action = pd.concat([df_basicunit_per_action, df_datacount_per_action], axis=1,join='inner').reset_index()
    return df_basicunit_per_action

def calculate_basicunit_per_mold_action(df_pre_groupby2):
    df_basicunit_mean_per_mold_action = df_pre_groupby2.groupby(["Mold_type","Action"])[["PowerConsumptionBasicUnit"]].mean().copy()
    df_basicunit_mean_per_mold_action.columns = ["PowerConsumptionBasicUnit(Average)"]
    df_basicunit_max_per_mold_action = df_pre_groupby2.groupby(["Mold_type","Action"])[["PowerConsumptionBasicUnit"]].max().copy()
    df_basicunit_max_per_mold_action.columns = ["PowerConsumptionBasicUnit(Max)"]
    df_basicunit_min_per_mold_action = df_pre_groupby2.groupby(["Mold_type","Action"])[["PowerConsumptionBasicUnit"]].min().copy()
    df_basicunit_min_per_mold_action.columns = ["PowerConsumptionBasicUnit(Min)"]
    df_basicunit_stdev_per_mold_action = df_pre_groupby2.groupby(["Mold_type","Action"])[["PowerConsumptionBasicUnit"]].std().copy()
    df_basicunit_stdev_per_mold_action.columns = ["PowerConsumptionBasicUnit(StandardDeviation)"]
    df_shotcount_sum_per_mold_action = df_pre_groupby2.groupby(["Mold_type","Action"])[["Shot_count"]].sum().copy()
    df_shotcount_sum_per_mold_action.columns = ["Shot_count(Sum)"]
    df_datacount_per_mold_action = df_pre_groupby2.groupby(["Mold_type","Action"]).size().copy()
    df_datacount_per_mold_action.name = "Data_count"

    df_basicunit_per_mold_action1 = pd.merge(df_basicunit_min_per_mold_action, df_basicunit_mean_per_mold_action, on=["Mold_type","Action"],how='outer')
    df_basicunit_per_mold_action2 = pd.merge(df_basicunit_per_mold_action1, df_basicunit_max_per_mold_action, on=["Mold_type","Action"],how='outer')
    df_basicunit_per_mold_action3 = pd.merge(df_basicunit_per_mold_action2, df_basicunit_stdev_per_mold_action, on=["Mold_type","Action"],how='outer')
    df_basicunit_per_mold_action4 = pd.merge(df_basicunit_per_mold_action3, df_shotcount_sum_per_mold_action, on=["Mold_type","Action"],how='outer')
    df_basicunit_per_mold_action5 = pd.merge(df_basicunit_per_mold_action4, df_datacount_per_mold_action, on=["Mold_type","Action"],how='outer').reset_index()
    return df_basicunit_per_mold_action5

def calculate_power_per_mold_action(df_pre_groupby2):
    df_basicunit_mean_per_mold_action = df_pre_groupby2.groupby(["Mold_type","Action"])[["PowerConsumption"]].mean().copy()
    df_basicunit_mean_per_mold_action.columns = ["PowerConsumption(Average)"]
    df_basicunit_max_per_mold_action = df_pre_groupby2.groupby(["Mold_type","Action"])[["PowerConsumption"]].max().copy()
    df_basicunit_max_per_mold_action.columns = ["PowerConsumption(Max)"]
    df_basicunit_min_per_mold_action = df_pre_groupby2.groupby(["Mold_type","Action"])[["PowerConsumption"]].min().copy()
    df_basicunit_min_per_mold_action.columns = ["PowerConsumption(Min)"]
    df_basicunit_stdev_per_mold_action = df_pre_groupby2.groupby(["Mold_type","Action"])[["PowerConsumption"]].std().copy()
    df_basicunit_stdev_per_mold_action.columns = ["PowerConsumption(StandardDeviation)"]
    df_shotcount_sum_per_mold_action = df_pre_groupby2.groupby(["Mold_type","Action"])[["Shot_count"]].sum().copy()
    df_shotcount_sum_per_mold_action.columns = ["Shot_count(Sum)"]
    df_datacount_per_mold_action = df_pre_groupby2.groupby(["Mold_type","Action"]).size().copy()
    df_datacount_per_mold_action.name = "Data_count"

    df_basicunit_per_mold_action1 = pd.merge(df_basicunit_min_per_mold_action, df_basicunit_mean_per_mold_action, on=["Mold_type","Action"],how='outer')
    df_basicunit_per_mold_action2 = pd.merge(df_basicunit_per_mold_action1, df_basicunit_max_per_mold_action, on=["Mold_type","Action"],how='outer')
    df_basicunit_per_mold_action3 = pd.merge(df_basicunit_per_mold_action2, df_basicunit_stdev_per_mold_action, on=["Mold_type","Action"],how='outer')
    df_basicunit_per_mold_action4 = pd.merge(df_basicunit_per_mold_action3, df_shotcount_sum_per_mold_action, on=["Mold_type","Action"],how='outer')
    df_basicunit_per_mold_action5 = pd.merge(df_basicunit_per_mold_action4, df_datacount_per_mold_action, on=["Mold_type","Action"],how='outer').reset_index()
    return df_basicunit_per_mold_action5

def calculate_basicunit_per_setup(df_pre_groupby3):
    df_pre_groupby3 = df_pre_groupby3[(df_pre_groupby3['Setup_flag'] == 1)].copy()
    df_basicunit_mean_per_action2 = df_pre_groupby3.groupby(["Setup_flag"])[["PowerConsumptionBasicUnit"]].mean().copy()
    df_basicunit_mean_per_action2.columns = ["PowerConsumptionBasicUnit(Average)"]
    df_basicunit_max_per_action2 = df_pre_groupby3.groupby(["Setup_flag"])[["PowerConsumptionBasicUnit"]].max().copy()
    df_basicunit_max_per_action2.columns = ["PowerConsumptionBasicUnit(Max)"]
    df_basicunit_min_per_action2 = df_pre_groupby3.groupby(["Setup_flag"])[["PowerConsumptionBasicUnit"]].min().copy()
    df_basicunit_min_per_action2.columns = ["PowerConsumptionBasicUnit(Min)"]
    df_basicunit_stdev_per_action2 = df_pre_groupby3.groupby(["Setup_flag"])[["PowerConsumptionBasicUnit"]].std().copy()
    df_basicunit_stdev_per_action2.columns = ["PowerConsumptionBasicUnit(StandardDeviation)"]
    df_shotcount_sum_per_action2 = df_pre_groupby3.groupby(["Setup_flag"])[["Shot_count"]].sum().copy()
    df_shotcount_sum_per_action2.columns = ["Shot_count(Sum)"]
    df_datacount_per_action2 = df_pre_groupby3.groupby(["Setup_flag"]).size().copy()
    df_datacount_per_action2.name = "Data_count"

    df_basicunit_per_action2 = pd.concat([df_basicunit_min_per_action2, df_basicunit_mean_per_action2], axis=1,join='inner')
    print(df_basicunit_per_action2)
    df_basicunit_per_action2 = pd.concat([df_basicunit_per_action2, df_basicunit_max_per_action2], axis=1,join='inner')
    print(df_basicunit_per_action2)
    df_basicunit_per_action2 = pd.concat([df_basicunit_per_action2, df_basicunit_stdev_per_action2], axis=1,join='inner')
    print(df_basicunit_per_action2)
    df_basicunit_per_action2 = pd.concat([df_basicunit_per_action2, df_shotcount_sum_per_action2], axis=1,join='inner')
    print(df_basicunit_per_action2)
    df_basicunit_per_action2 = pd.concat([df_basicunit_per_action2, df_datacount_per_action2], axis=1,join='inner')
    print(df_basicunit_per_action2)
    return df_basicunit_per_action2

def calculate_basicunit_per_mold_setup(df_pre_groupby3):
    df_basicunit_mean_per_mold_action2 = df_pre_groupby3.groupby(["Mold_type"])[["PowerConsumptionBasicUnit"]].mean().copy()
    df_basicunit_mean_per_mold_action2.columns = ["PowerConsumptionBasicUnit(Average)"]
    df_basicunit_max_per_mold_action2 = df_pre_groupby3.groupby(["Mold_type"])[["PowerConsumptionBasicUnit"]].max().copy()
    df_basicunit_max_per_mold_action2.columns = ["PowerConsumptionBasicUnit(Max)"]
    df_basicunit_min_per_mold_action2 = df_pre_groupby3.groupby(["Mold_type"])[["PowerConsumptionBasicUnit"]].min().copy()
    df_basicunit_min_per_mold_action2.columns = ["PowerConsumptionBasicUnit(Min)"]
    df_basicunit_stdev_per_mold_action2 = df_pre_groupby3.groupby(["Mold_type"])[["PowerConsumptionBasicUnit"]].std().copy()
    df_basicunit_stdev_per_mold_action2.columns = ["PowerConsumptionBasicUnit(StandardDeviation)"]
    df_shotcount_sum_per_mold_action2 = df_pre_groupby3.groupby(["Mold_type"])[["Shot_count"]].sum().copy()
    df_shotcount_sum_per_mold_action2.columns = ["Shot_count(Sum)"]
    df_datacount_per_mold_action2 = df_pre_groupby3.groupby(["Mold_type"]).size().copy()
    df_datacount_per_mold_action2.name = "Data_count"

    df_basicunit_per_mold_action1_2 = pd.merge(df_basicunit_min_per_mold_action2, df_basicunit_mean_per_mold_action2, on=["Mold_type"],how='outer')
    df_basicunit_per_mold_action2_2 = pd.merge(df_basicunit_per_mold_action1_2, df_basicunit_max_per_mold_action2, on=["Mold_type"],how='outer')
    df_basicunit_per_mold_action3_2 = pd.merge(df_basicunit_per_mold_action2_2, df_basicunit_stdev_per_mold_action2, on=["Mold_type"],how='outer')
    df_basicunit_per_mold_action4_2 = pd.merge(df_basicunit_per_mold_action3_2, df_shotcount_sum_per_mold_action2, on=["Mold_type"],how='outer')
    df_basicunit_per_mold_action5_2 = pd.merge(df_basicunit_per_mold_action4_2, df_datacount_per_mold_action2, on=["Mold_type"],how='outer').reset_index()
    return df_basicunit_per_mold_action5_2

def make_figure_per_month(df_pre_groupby4,month):
    plt.figure(figsize=(10,5))
    if month ==4:
        df_time_202004b = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,4,1)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,4,11))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202004b, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-04B_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==5:
        df_time_202005b = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,5,1)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,5,11))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202005b, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-05B_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==6:
        df_time_202006b = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,6,1)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,6,11))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202006b, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-06B_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==7:
        df_time_202007b = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,7,1)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,7,11))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202007b, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-07B_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==8:
        df_time_202008b = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,8,1)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,8,11))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202008b, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-08B_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==9:
        df_time_202009b = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,9,1)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,9,11))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202009b, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-09B_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==10:
        df_time_202010b = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,10,1)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,10,11))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202010b, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-10B_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==11:
        df_time_202011b = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,11,1)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,11,11))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202011b, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-11B_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==12:
        df_time_202012b = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,12,1)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,12,11))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202012b, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-12B_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==1:
        df_time_202101b = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2021,1,1)) & (df_pre_groupby4['DateTime'] < dt.datetime(2021,1,11))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202101b, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2021-01B_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==2:
        df_time_202102b = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2021,2,1)) & (df_pre_groupby4['DateTime'] < dt.datetime(2021,2,11))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202102b, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2021-02B_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    else: #month ==3:
        df_time_202103b = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2021,3,1)) & (df_pre_groupby4['DateTime'] < dt.datetime(2021,3,11))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202103b, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2021-03B_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)

    plt.figure(figsize=(10,5))
    if month ==4:
        df_time_202004m = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,4,11)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,4,21))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202004m, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-04M_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==5:
        df_time_202005m = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,5,11)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,5,21))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202005m, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-05M_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==6:
        df_time_202006m = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,6,11)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,6,21))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202006m, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-06M_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==7:
        df_time_202007m = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,7,11)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,7,21))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202007m, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-07M_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==8:
        df_time_202008m = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,8,11)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,8,21))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202008m, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-08M_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==9:
        df_time_202009m = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,9,11)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,9,21))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202009m, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-09M_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==10:
        df_time_202010m = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,10,11)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,10,21))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202010m, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-10M_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==11:
        df_time_202011m = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,11,11)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,11,21))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202011m, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-11M_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==12:
        df_time_202012m = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,12,11)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,12,21))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202012m, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-12M_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==1:
        df_time_202101m = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2021,1,11)) & (df_pre_groupby4['DateTime'] < dt.datetime(2021,1,21))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202101m, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2021-01M_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==2:
        df_time_202102m = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2021,2,11)) & (df_pre_groupby4['DateTime'] < dt.datetime(2021,2,21))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202102m, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2021-02M_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    else: #month ==3:
        df_time_202103m = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2021,3,11)) & (df_pre_groupby4['DateTime'] < dt.datetime(2021,3,21))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202103m, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2021-03M_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)

    plt.figure(figsize=(10,5))
    if month ==4:
        df_time_202004e = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,4,21)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,5,1))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202004e, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-04E_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==5:
        df_time_202005e = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,5,21)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,6,1))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202005e, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-05E_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==6:
        df_time_202006e = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,6,21)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,7,1))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202006e, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-06E_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==7:
        df_time_202007e = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,7,21)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,8,1))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202007e, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-07E_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==8:
        df_time_202008e = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,8,21)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,9,1))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202008e, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-08E_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==9:
        df_time_202009e = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,9,21)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,10,1))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202009e, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-09E_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==10:
        df_time_202010e = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,10,21)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,11,1))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202010e, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-10E_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==11:
        df_time_202011e = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,11,21)) & (df_pre_groupby4['DateTime'] < dt.datetime(2020,12,1))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202011e, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-11E_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==12:
        df_time_202012e = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2020,12,21)) & (df_pre_groupby4['DateTime'] < dt.datetime(2021,1,1))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202012e, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2020-12E_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==1:
        df_time_202101e = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2021,1,21)) & (df_pre_groupby4['DateTime'] < dt.datetime(2021,2,1))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202101e, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2021-01E_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    elif month ==2:
        df_time_202102e = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2021,2,21)) & (df_pre_groupby4['DateTime'] < dt.datetime(2021,3,1))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202102e, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2021-02E_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)
    else: #month ==3:
        df_time_202103e = df_pre_groupby4[(df_pre_groupby4['DateTime'] >= dt.datetime(2021,3,21)) & (df_pre_groupby4['DateTime'] < dt.datetime(2021,4,1))]
        g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_time_202103e, style="Mold_type")
        plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('./OutputData/Figure/Time_BasicUnit/scatterplot_2021-03E_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)

def make_figure_data_count(df_power_loss_per_hour):
    # df_power_loss_per_hour.groupby("Mold_type").get_group("Falcon")
    for mold_type, df_temp in df_power_loss_per_hour.groupby("Mold_type"): # Mold Type毎にDataFrameを分割
        df_per_hour = df_power_loss_per_hour.groupby("Mold_type").get_group(mold_type)
        print(df_per_hour)
        df_datacount = df_per_hour.groupby(["Mold_type"]).size().copy()
        df_datacount.name = "Data_count"
        df_per_hour = pd.merge(df_per_hour, df_datacount, on=["Mold_type"],how='outer').reset_index()
        plt.figure(figsize=(10,5))
        # sns.scatterplot(x='PowerConsumptionBasicUnit',y='Data_count', hue="Mold_type", data=df_per_hour, style="Mold_type")
        # df_per_hour['PowerConsumptionBasicUnit'].hist(bins=500,title=mold_type,log=True)
        mold_type = mold_type.replace(" ", "")
        mold_type.lstrip()
        mold_type = mold_type.strip(" ")
        df_per_hour['PowerConsumptionBasicUnit'].plot(kind='hist',bins=500,range=(0,5),title=mold_type,log=True)
        plt.grid(axis='y', color='k', linestyle='dotted', linewidth=0.1)
        plt.xlabel("PowerConsumptionBasicUnit", {"fontsize": 20})
        plt.ylabel("Data_count", {"fontsize": 20})
        # plt.hist(x='PowerConsumptionBasicUnit', bins=500, range=None, bottom=None, histtype='bar',align='mid', orientation='vertical', rwidth=None,log=False, label="Mold_type", stacked=False, hold=None, data=df_per_hour)
        plt.legend(loc='upper left',bbox_to_anchor=(0.6, 1))
        plt.savefig('./OutputData/Figure/DataCount_BasicUnit/scatterplot_'+ mold_type + "_DataCount_PowerConsumptionBasicUnit.png", dpi = 200)

def make_figure_violinplot(df_per_day_mold):
    #ヴァイオリンプロット保存設定
    violinplot_output_file_name ='seaborn_violinplot_Startup_loss_time.png'
    violinplot_dirname = "./OutputData/Figure/violinplot/"

    #ヴァイオリンプロット出力(2021/9/9 追加)
    df_per_day_mold_merged1 = df_per_day_mold.loc[df_per_day_mold["Startup_loss_time"]>0]
    plt.figure(figsize=(10,5))
    sns.violinplot(x=df_per_day_mold_merged1["Mold_type"], y=df_per_day_mold_merged1['Startup_loss_time'])
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig(violinplot_dirname + '/' + violinplot_output_file_name, dpi = 200)

    plt.figure(figsize=(10,5))
    df_per_day_mold_merged2 = df_per_day_mold.loc[df_per_day_mold["Setup_and_wait_loss_time"]>0]
    sns.violinplot(x=df_per_day_mold_merged2["Mold_type"], y=df_per_day_mold_merged2['Setup_and_wait_loss_time'])
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig('./OutputData/Figure/violinplot/seaborn_violinplot_Setup_and_wait_loss_time.png', dpi = 200)

    plt.figure(figsize=(10,5))
    df_per_day_mold_merged2 = df_per_day_mold.loc[df_per_day_mold["Setup_and_run_loss_time"]>0]
    sns.violinplot(x=df_per_day_mold_merged2["Mold_type"], y=df_per_day_mold_merged2['Setup_and_run_loss_time'])
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig('./OutputData/violinplot/seaborn_violinplot_Setup_and_run_loss_time.png', dpi = 200)

    plt.figure(figsize=(10,5))
    df_per_day_mold_merged3 = df_per_day_mold.loc[df_per_day_mold["Run_loss_time"]>0]
    sns.violinplot(x=df_per_day_mold_merged3["Mold_type"], y=df_per_day_mold_merged3['Run_loss_time'])
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig('./OutputData/Figure/violinplot/seaborn_violinplot_Run_loss_time.png', dpi = 200)

    plt.figure(figsize=(10,5))
    df_per_day_mold_merged4 = df_per_day_mold.loc[df_per_day_mold["Startup_loss"]>0]
    sns.violinplot(x=df_per_day_mold_merged4["Mold_type"], y=df_per_day_mold_merged4['Startup_loss'])
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig('./OutputData/Figure/violinplot/seaborn_violinplot_Startup_loss.png', dpi = 200)

    plt.figure(figsize=(10,5))
    df_per_day_mold_merged5 = df_per_day_mold.loc[df_per_day_mold["Setup_and_wait_loss"]>0]
    sns.violinplot(x=df_per_day_mold_merged5["Mold_type"], y=df_per_day_mold_merged5['Setup_and_wait_loss'])
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig('./OutputData/Figure/violinplot/seaborn_violinplot_Setup_and_wait_loss.png', dpi = 200)

    plt.figure(figsize=(10,5))
    df_per_day_mold_merged5 = df_per_day_mold.loc[df_per_day_mold["Setup_and_run_loss"]>0]
    sns.violinplot(x=df_per_day_mold_merged5["Mold_type"], y=df_per_day_mold_merged5['Setup_and_run_loss'])
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig('./OutputData/Figure/violinplot/seaborn_violinplot_Setup_and_run_loss.png', dpi = 200)

    plt.figure(figsize=(10,5))
    df_per_day_mold_merged6 = df_per_day_mold.loc[df_per_day_mold["Run_loss"]>0]
    sns.violinplot(x=df_per_day_mold_merged6["Mold_type"], y=df_per_day_mold_merged6['Run_loss'])
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig('./OutputData/Figure/violinplot/seaborn_violinplot_Run_loss.png', dpi = 200)

    plt.figure(figsize=(10,5))
    df_per_day_mold_merged7 = df_per_day_mold.loc[df_per_day_mold["Down_loss_time"]>0]
    sns.violinplot(x=df_per_day_mold_merged7["Mold_type"], y=df_per_day_mold_merged7['Down_loss_time'])
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig('./OutputData/Figure/violinplot/seaborn_violinplot_Down_loss_time.png', dpi = 200)

    plt.figure(figsize=(10,5))
    df_per_day_mold_merged8 = df_per_day_mold.loc[df_per_day_mold["Down_loss"]>0]
    sns.violinplot(x=df_per_day_mold_merged8["Mold_type"], y=df_per_day_mold_merged8['Down_loss'])
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig('./OutputData/Figure/violinplot/seaborn_violinplot_Down_loss.png', dpi = 200)

    plt.figure(figsize=(10,5))
    df_per_day_mold_merged9 = df_per_day_mold.loc[df_per_day_mold["PowerConsumptionBasicUnit"]>0]
    sns.violinplot(x=df_per_day_mold_merged9["Mold_type"], y=df_per_day_mold_merged9['PowerConsumptionBasicUnit'])
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig('./OutputData/Figure/violinplot/seaborn_violinplot_PowerConsumptionBasicUnit.png', dpi = 200)

def make_figure_barplot(df_per_year_mold_output):
    plt.figure(figsize=(10,5))
    s1 = sns.barplot(x = "Mold_type", y = 'Run_loss', data = df_per_year_mold_output, color = 'red')
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig('./OutputData/Figure/barplot/barplot_Mold_type_Run_loss.png', dpi = 200)

    plt.figure(figsize=(10,5))
    sns.barplot(x = "Mold_type", y = 'Setup_and_wait_loss', data = df_per_year_mold_output, color = 'blue')
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig('./OutputData/Figure/barplot/barplot_Mold_type_Setup_and_wait_loss.png', dpi = 200)

    plt.figure(figsize=(10,5))
    # fig, (ax1, ax2) = plt.subplots(1, 2)
    # plt.xticks(rotation=90)
    # plt.tight_layout()
    sns.barplot(x = "Mold_type", y = 'Setup_and_run_loss', data = df_per_year_mold_output, color = 'blue')
    # plt.xticks(rotation=90)
    # plt.tight_layout()
    # sns.barplot(x = "Mold_type", y = 'Setup_and_wait_loss', data = df_per_year_mold_output, color = 'blue', ax=ax2)
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig('./OutputData/Figure/barplot/barplot_Mold_type_Setup_and_run_loss.png', dpi = 200)

def make_figure_scatterplot_basicunit(df_power_loss_per_hour):
    #時系列(全期間)の電力量原単位グラフ出力(2021/9/22 追加)
    plt.figure(figsize=(10,5))
    g = sns.scatterplot(x='DateTime',y='PowerConsumptionBasicUnit', hue="Mold_type", data=df_power_loss_per_hour, style="Mold_type")
    # g.despine(left=True)
    plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
    plt.savefig('./OutputData/Figure/scatterplot_BasicUnit/scatterplot_DateTime_PowerConsumptionBasicUnit_by_mold.png', dpi = 200)

def make_figure_scatterplot_shotcount_power(df_per_day_merged):
    #散布図出力(2021/9/9 追加)
    plt.figure(figsize=(10,5))
    df_per_day_merged.plot.scatter(x='Shot_count',y='PowerConsumption')
    plt.savefig('./OutputData/Figure/scatterplot_ShotCount_Power/scatterplot_Shot_count_PowerConsumption.png', dpi = 200)

def make_figure_scatterplot_shotcount_power_by_mold(df_per_day_mold):
    #散布図出力(2021/9/9 追加)
    plt.figure(figsize=(10,5))
    g = sns.scatterplot(x='Shot_count',y='PowerConsumption', hue="Mold_type", data=df_per_day_mold, style="Mold_type")
    # g.despine(left=True)
    plt.legend(loc='upper left',bbox_to_anchor=(0.95, 1))
    plt.savefig('./OutputData/Figure/scatterplot_ShotCount_Power_by_mold/scatterplot_Shot_count_PowerConsumption_by_mold.png', dpi = 200)

def runloss_calculate_per_mold_action(df_basicunit_per_mold_action,df_per_hour):

    print(df_basicunit_per_mold_action)
    print(type(df_basicunit_per_mold_action))
    for index, df_row_per_hour in df_per_hour.iterrows():
        mold_type = df_row_per_hour["Mold_type"]
        action = df_row_per_hour["Action"]
        setup = df_row_per_hour["Setup_flag"]
        if (action == 4 or action == 8 or action == 12) and setup == 0:
            for index_standard, df_row_standard in df_basicunit_per_mold_action.iterrows():
                if df_row_standard["Mold_type"] == mold_type and df_row_standard["Action"] == action:
                    standard_basic_unit = df_row_standard["PowerConsumptionBasicUnit(Min)"]
                    run_loss = (df_row_per_hour["PowerConsumptionBasicUnit"] - standard_basic_unit)*df_row_per_hour["Shot_count"]
                    df_per_hour.at[index, "Run_loss"] = run_loss
                else:
                    pass
        else:
            df_per_hour.at[index, "Run_loss"] = 0

    df_modified_runloss_per_hour = df_per_hour.copy()

    return df_modified_runloss_per_hour

def setuploss_calculate_per_mold_action(df_setup_basicunit_per_mold,df_per_hour):

    print(df_setup_basicunit_per_mold)
    print(type(df_setup_basicunit_per_mold))
    for index, df_row_per_hour in df_per_hour.iterrows():
        mold_type = df_row_per_hour["Mold_type"]
        action = df_row_per_hour["Action"]
        if df_row_per_hour["Setup_flag"]==1: 
            for df_row_standard in df_setup_basicunit_per_mold.itertuples():
                # if df_row_standard["Mold_type"] == mold_type:
                if df_row_standard.Mold_type == mold_type:
                    standard_basic_unit = df_row_standard._2
                    setup_loss = (df_row_per_hour["PowerConsumptionBasicUnit"] - standard_basic_unit)*df_row_per_hour["Shot_count"]
                    df_per_hour.at[index, "Setup_and_run_loss"] = setup_loss
                    # standard_basic_unit = df_row_standard["PowerConsumptionBasicUnit(Min)"]
                    # setup_loss = (df_row_per_hour["PowerConsumptionBasicUnit"] - standard_basic_unit)*df_row_per_hour["Shot_count"]
                    # df_per_hour.at[index, "Setup_loss"] = setup_loss
                else:
                    pass
        else:
            df_per_hour.at[index, "Setup_and_run_loss"] = 0

    df_modified_setuploss_per_hour = df_per_hour.copy()

    return df_modified_setuploss_per_hour

def startuploss_calculate_per_mold_action(df_basicunit_per_mold_action,df_per_hour):
    
    print(df_basicunit_per_mold_action)
    print(type(df_basicunit_per_mold_action))
    oldaction = 0
    for index, df_row_per_hour in df_per_hour.iterrows():
        mold_type = df_row_per_hour["Mold_type"]
        action = df_row_per_hour["Action"]
        setup = df_row_per_hour["Setup_flag"]
        if action == 3 and setup == 0:
            for index_standard, df_row_standard in df_basicunit_per_mold_action.iterrows():
                if df_row_standard["Mold_type"] == mold_type and df_row_standard["Action"] == action:
                    standard_power = df_row_standard["PowerConsumption(Min)"]
                    run_loss = df_row_per_hour["PowerConsumption"] - standard_power
                    df_per_hour.at[index, "Startup_loss"] = run_loss
                else:
                    pass
            oldaction = action
        elif action == 3 and setup == 1:
            for index_standard, df_row_standard in df_basicunit_per_mold_action.iterrows():
                if df_row_standard["Mold_type"] == mold_type and df_row_standard["Action"] == action:
                    # standard_power = df_row_standard["PowerConsumption(Min)"]
                    # run_loss = df_row_per_hour["PowerConsumption"] - standard_power
                    df_per_hour.at[index, "Startup_loss"] = 0
                else:
                    df_per_hour.at[index, "Startup_loss"] = 0
            oldaction = action
        elif (action != 5 and action != 6 and action != 8) and oldaction == 3 and setup == 0:
            for index_standard, df_row_standard in df_basicunit_per_mold_action.iterrows():
                if df_row_standard["Mold_type"] == mold_type and df_row_standard["Action"] == action:
                    standard_power = df_row_standard["PowerConsumption(Min)"]
                    run_loss = df_row_per_hour["PowerConsumption"] - standard_power
                    df_per_hour.at[index, "Startup_loss"] = run_loss
                else:
                    pass
        elif (action != 5 and action != 6 and action != 8) and oldaction == 3 and setup == 1:
            for index_standard, df_row_standard in df_basicunit_per_mold_action.iterrows():
                if df_row_standard["Mold_type"] == mold_type and df_row_standard["Action"] == action:
                    # standard_power = df_row_standard["PowerConsumption(Min)"]
                    # run_loss = df_row_per_hour["PowerConsumption"] - standard_power
                    df_per_hour.at[index, "Startup_loss"] = 0
                else:
                    pass
        elif (action == 5 or action == 6 or action == 8) and oldaction == 3:
            oldaction = action
            df_per_hour.at[index, "Startup_loss"] = 0
        else:
            df_per_hour.at[index, "Startup_loss"] = 0

    df_modified_runloss_per_hour = df_per_hour.copy()

    return df_modified_runloss_per_hour

def downloss_calculate_per_mold_action(df_basicunit_per_mold_action,df_per_hour):
    
    print(df_basicunit_per_mold_action)
    print(type(df_basicunit_per_mold_action))
    oldaction = 0
    for index, df_row_per_hour in df_per_hour.iterrows():
        mold_type = df_row_per_hour["Mold_type"]
        action = df_row_per_hour["Action"]
        setup = df_row_per_hour["Setup_flag"]
        if action == 11 and setup == 0:
            for index_standard, df_row_standard in df_basicunit_per_mold_action.iterrows():
                if df_row_standard["Mold_type"] == mold_type and df_row_standard["Action"] == action:
                    standard_power = df_row_standard["PowerConsumption(Min)"]
                    run_loss = df_row_per_hour["PowerConsumption"] - standard_power
                    df_per_hour.at[index, "Down_loss"] = run_loss
                else:
                    pass
            oldaction = action
        elif action == 11 and setup == 1:
            for index_standard, df_row_standard in df_basicunit_per_mold_action.iterrows():
                if df_row_standard["Mold_type"] == mold_type and df_row_standard["Action"] == action:
                    # standard_power = df_row_standard["PowerConsumption(Min)"]
                    # run_loss = df_row_per_hour["PowerConsumption"] - standard_power
                    df_per_hour.at[index, "Down_loss"] = 0
                else:
                    df_per_hour.at[index, "Down_loss"] = 0
            oldaction = action
        elif (action != 5 and action != 6 and action != 8) and oldaction == 11 and setup == 0:
            for index_standard, df_row_standard in df_basicunit_per_mold_action.iterrows():
                if df_row_standard["Mold_type"] == mold_type and df_row_standard["Action"] == action:
                    standard_power = df_row_standard["PowerConsumption(Min)"]
                    run_loss = df_row_per_hour["PowerConsumption"] - standard_power
                    df_per_hour.at[index, "Down_loss"] = run_loss
                else:
                    pass
        elif (action != 5 and action != 6 and action != 8) and oldaction == 11 and setup == 1:
            for index_standard, df_row_standard in df_basicunit_per_mold_action.iterrows():
                if df_row_standard["Mold_type"] == mold_type and df_row_standard["Action"] == action:
                    # standard_power = df_row_standard["PowerConsumption(Min)"]
                    # run_loss = df_row_per_hour["PowerConsumption"] - standard_power
                    df_per_hour.at[index, "Down_loss"] = 0
                else:
                    pass
        elif (action == 5 or action == 6 or action == 8) and oldaction == 11:
            oldaction = action
            df_per_hour.at[index, "Down_loss"] = 0
        else:
            df_per_hour.at[index, "Down_loss"] = 0

    df_modified_runloss_per_hour = df_per_hour.copy()

    return df_modified_runloss_per_hour

#---------------------------------------------------
#   生産数量+電力量のDataFrameにリサンプルした運転状態(1日)のdfを結合
#---------------------------------------------------
# def dataframe_join_per_day(df_per_day_basic_unit,df_per_day_setup_state_resumpled):
#     # df_merge = pd.merge(df_production,df_electric_power,on='DateTime')

#     insert_column_number = df_per_day_basic_unit.columns.get_loc('CO2emissionsCostBasicUnit') + 1

#     # 'PowerConsumption' 'CO2emissions' 'ElectricPowerCost' 'CO2emissionsCost'のcolumnの移動
#     # insert_column_number = df_merge.columns.get_loc('ProductionQuantity') + 1 #2021/8/26 コメント化
#     insert_column_temp = df_merge.pop('PowerConsumption')
#     df_merge.insert(insert_column_number,'PowerConsumption',insert_column_temp)
#     insert_column_temp = df_merge.pop('CO2emissions')
#     df_merge.insert(insert_column_number+1,'CO2emissions',insert_column_temp)
#     insert_column_temp = df_merge.pop('ElectricPowerCost')
#     df_merge.insert(insert_column_number+2,'ElectricPowerCost',insert_column_temp)
#     insert_column_temp = df_merge.pop('CO2emissionsCost')
#     df_merge.insert(insert_column_number+3,'CO2emissionsCost',insert_column_temp)
#     return df_merge

#def resample_production_number_per_day(production_line, df_production_number):
    # 日毎のライン毎の原単位算出
    # 生産していないにも関わらず、電力消費している箇所を抽出する
    # 日毎の型式毎の1個当たりの消費電力量を類推⇒重回帰分析にて

# 直接実行されたとき、メイン関数呼び出し
if __name__ == '__main__':
	main.main()