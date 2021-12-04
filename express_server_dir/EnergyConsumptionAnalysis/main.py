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
import ProductionAnalysis as pa
import ElectricPowerAnalysis as ea
import CorrelationAnalysis as ca
import MultipleRegressionAnalysis as ma
import DailyWorkReport as dr

import matplotlib.pyplot as plt
import seaborn as sns


os.chdir(os.path.dirname(os.path.abspath(__file__)))

# # cgi用諸設定
# import cgitb
# cgitb.enable()
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding = 'utf-8')
# print('Content-Type: text/html; charset=UTF-8\n')
local_root_url=os.getcwd()

# ディレクトリを指定
electric_power_consumption_dirname = "./InputData/ElectricPower"
production_quantity_dirname = "./InputData/Production"
electric_power_data_agg_dirname = "./OutputData/ElectricPower"
production_quantity_data_agg_dirname = "./OutputData/ProductionQuantity/old"
correlation_analysis_dirname = "./OutputData/CorrelationAnalysis"
multi_regression_analysis_dirname = "./OutputData/MultipleRegression/MultipleRegression"
pre_multi_regression_analysis_dirname = "./OutputData/MultipleRegression/PreMultipleRegression"
fig_violinplot_dirname = "./OutputData/Figure/violinplot"
fig_barplot_dirname = "./OutputData/Figure/barplot"
fig_datacount_basicunit_dirname = "./OutputData/Figure/DataCount_BasicUnit"
fig_time_basicunit_dirname = "./OutputData/Figure/Time_BasicUnit"
fig_scatterplot_basicunit_dirname = "./OutputData/Figure/scatterplot_BasicUnit"
fig_scatterplot_shotcount_power_dirname = "./OutputData/Figure/scatterplot_ShotCount_Power"
fig_scatterplot_shotcount_power_by_mold_dirname = "./OutputData/Figure/scatterplot_ShotCount_Power_by_mold"
report_input_dirname = "./InputData/DailyWorkReport"
report_output_dirname = "./OutputData/DailyWorkReport"

# 1時間単位生産数量データ読み込み追加
line_number="No24"
production_qty_per_hour_dirname = "./InputData/Production/"+line_number 
production_qty_data_agg_per_hour_dirname = "./OutputData/ProductionQuantity/ProductionQuantity_per_hour/"+line_number 
production_qty_data_agg_per_hour_not_yet_resample_dirname = "./OutputData/ProductionQuantity/Production_not_yet_resample"
production_qty_data_agg_per_day_dirname = "./OutputData/ProductionQuantity/ProductionQuantity_per_day/"+line_number 

# 1次分析データ（電力消費量、生産数量、1時間単位生産数量）読み込み
electric_power_analysis_file = os.listdir(electric_power_data_agg_dirname)
production_quantity_analysis_file = os.listdir(production_quantity_data_agg_dirname)
production_qty_per_hour_analysis_files = os.listdir(production_qty_data_agg_per_hour_dirname)

def to_time_series_data():
        #-------------------------------------------------------------入力データを時系列データ化-------------------------------------------------------------#
    if len(production_quantity_analysis_file) == 0:
        #入力データファイル読み込み
        production_quantity_dirname_file = os.listdir(production_quantity_dirname)
        #生産数量読み込みファイルパス
        production_quantity_dirname_file_path = production_quantity_dirname + "/" + production_quantity_dirname_file[0]
        #Pandasでファイル読み込み       
        production_sheet = pd.read_excel(production_quantity_dirname_file_path, header=None, index_col=None,names=['ProductionLine','Product','ProductionQuantity','DateTime', 'ProductType','Number_of_stacked_cores','Number_of_Production'], skiprows=[0])
        #2021/7/16Temporary処理無効化
        #生産数量ファイル保存設定
        production_quantity_output_file_name ='Production_Quantity_Analysis.xlsx'
        production_quantity_output_file = pd.ExcelWriter(production_quantity_data_agg_dirname + "/" + production_quantity_output_file_name)

        #2021/7/16Temporary処理無効化
        #各ライン毎の生産数量計算
        df_production_number = pa.production_number_calculte(production_sheet)

        production_line, df_production_number_per_line = pa.production_number_data_sheet_create(production_quantity_output_file,df_production_number)

        #2021/7/16Temporary処理無効化
        #生産機種の分類
        if production_line == str("24SC"): # 現状はNo.24SCラインしか電力量のデータないためターゲット絞って分析
            #pa.product_type_classify(df_production_number_per_line)
            df_production_analysis, df_production_per_product = pa.product_type_classify(df_production_number_per_line)
            df_production_analysis.to_excel(production_quantity_output_file, str(production_line + "_ProductionAnalysis"), index = False)
            df_production_per_product.to_excel(production_quantity_output_file, str(production_line + "_ProductionOrder"), index = False)
        else:
            pass

        #2021/7/16Temporary処理無効化
        # 生産数量ファイル保存
        production_quantity_output_file.save()
    else:
        print("=======Production_Pass=============\n")
        pass

    #時間毎生産数量ファイル保存設定
    production_qty_per_hour_output_file_name ='[Confidential]Production_Quantity_per_hour_Analysis.xlsx'
    production_qty_per_hour_output_file = pd.ExcelWriter(production_qty_data_agg_per_hour_dirname + "/" + production_qty_per_hour_output_file_name)
    #2021/8/25 1時間単位生産数量データ読み込み追加
    if len(production_qty_per_hour_analysis_files) == 0:
        #入力データファイル読み込み
        production_qty_per_hour_dirname_file = os.listdir(production_qty_per_hour_dirname)
        #生産数量読み込みファイルパス
        # production_qty_per_hour_dirname_file_path = production_qty_per_hour_dirname + "/" + production_qty_per_hour_dirname_file[0]

        #生産数量csvファイル読み込み
        prodution_file_list = []
        for p in production_qty_per_hour_dirname_file:
            filename = p
	        #Pandasでファイル読み込み 
            df = pd.read_csv(production_qty_per_hour_dirname + "/" + filename,encoding = "shift-jis",header=2,names=['DateTime','ProductionLine','a','Mold_type','b','c','Shot_count','d','e'],index_col=0,parse_dates=[0])#index_col=0,parse_dates=[0])
            df = df.iloc[:,:] # ヘッダーの余計な部分（空白行など）を除いて上書き(念のため)
            prodution_file_list.append(df) # listに要素(df)を追加していく
            p = []
        df_not_yet_resumple = pd.concat(prodution_file_list,axis=0).drop(df.columns[[6, 7]], axis=1).drop(df.columns[[3, 4]], axis=1).drop(df.columns[1], axis=1) # axis=0 (時間軸方向）に結合
        df_not_yet_resumple = df_not_yet_resumple.sort_values(by=['DateTime']) #DateTimeで降順sort

        #生産数量ファイル保存設定
        production_qty_per_hour_not_yet_resample_output_file_name ='Product_per_hour_not_yet_resumple.xlsx'
        production_qty_per_hour_not_yet_resample_output_file = pd.ExcelWriter(production_qty_data_agg_per_hour_not_yet_resample_dirname  + "/" + production_qty_per_hour_not_yet_resample_output_file_name)

        # 生産数量ファイル保存
        df_not_yet_resumple.to_excel(production_qty_per_hour_not_yet_resample_output_file)
        production_qty_per_hour_not_yet_resample_output_file.save()

        #生産数量データの1時間単位のResampling
        df_product_qty_per_hour = pa.production_per_hour_calculate(df_not_yet_resumple)
        #各ライン毎の生産数量計算
        #df_production_number = pa.production_number_calculte(production_sheet) #2021/8/25無効化

        #production_line, df_production_number_per_line = pa.production_number_data_sheet_create(production_quantity_output_file,df_production_number) #2021/8/25無効化

        #生産機種の分類
        #if production_line == str("24SC"): # 現状はNo.24SCラインしか電力量のデータないためターゲット絞って分析
        #    #pa.product_type_classify(df_production_number_per_line) #2021/8/25無効化
        #   df_production_analysis, df_production_per_product = pa.product_type_classify(df_production_number_per_line) #2021/8/25無効化
        #    df_production_analysis.to_excel(production_quantity_output_file, str(production_line + "_ProductionAnalysis"), index = False) #2021/8/25無効化
        #    df_production_per_product.to_excel(production_quantity_output_file, str(production_line + "_ProductionOrder"), index = False) #2021/8/25無効化
        #else:
        #    pass
        
        # 生産数量ファイル保存
        df_product_qty_per_hour.to_excel(production_qty_per_hour_output_file)
        production_qty_per_hour_output_file.save()
    else:
        print("=======Production_per_hour_Pass=============\n")
        pass
    
    #電力量ファイル保存設定
    electric_power_output_file_name ='Electric_Power_Analysis.xlsx'
    electric_power_output_file = pd.ExcelWriter(electric_power_data_agg_dirname + "/" + electric_power_output_file_name)
    if len(electric_power_analysis_file) == 0:
        # 入力データファイル読み込み
        electric_power_consumption_file = os.listdir(electric_power_consumption_dirname)
        #電力量読み込みファイルパス
        electric_power_consumption_file_path = electric_power_consumption_dirname + "/" + electric_power_consumption_file[0]
        # Pandasでファイル読み込み
        dict_electric_sheet_all = pd.read_excel(electric_power_consumption_file_path, sheet_name=None, header=None, names=['DateTime','PowerConsumption'], skiprows=[0,1], skipfooter=8)

        #1時間毎の電力量計算
        df_electric_power_per_hour = ea.electric_power_per_hour_calculate(dict_electric_sheet_all)
        #1時間毎の電力量コスト計算s
        df_electric_power_cost_per_hour = ea.electric_power_cost_per_hour_calculate(df_electric_power_per_hour)
        df_electric_power_cost_per_hour.to_excel(electric_power_output_file, "electric_power", index = False)
        #1日毎の電力量計算
        df_electric_power_per_day, df_electric_power_for_analysis = ea.electric_power_per_day_calculate(df_electric_power_cost_per_hour)
        df_electric_power_per_day.to_excel(electric_power_output_file, "electric_power_analysis", index = False)
        #重回帰分析用
        df_electric_power_for_analysis.to_excel(electric_power_output_file, "MultiRegressionAnalysis", index = False)
        #電力量ファイル保存
        electric_power_output_file.save()
    else:
        print("=======Electric_Power_Pass=============\n")
        pass

    # # 1次分析データ読み込み
    # electric_power_analysis_file = os.listdir(electric_power_data_agg_dirname)
    # production_quantity_analysis_file = os.listdir(production_quantity_data_agg_dirname)
    # production_quantity_per_hour_analysis_file = os.listdir(production_qty_data_agg_per_hour_dirname)  #2021/08/26 追記 dir → "./OutputData/ProductionQuantity_per_hour/No24"

    # データファイル存在確認(2021/10/1 追加 菊地)
    report_output_file_list = os.listdir(report_output_dirname)
    if len(report_output_file_list) == 0:
        # 日報情報時系列データ化(2021/10/1 追加 菊地)
        df_worker_final = dr.dailywork_report()
    else:
        print("=======worker_report_Pass=============\n")
        pass

    #1日毎日報情報時系列データファイルパス(2021/10/1 追加)
    report_output_file_name ='[Confidential]Work_report_per_day.xlsx'
    report_output_file = report_output_dirname + "/" + report_output_file_name
    #1日毎日報情報時系列データファイルをPandasで呼び出し(2021/10/1 追加)
    df_worker_final = pd.read_excel(report_output_file, index_col=None)
    print("=======df_worker_final=============\n")
    print(df_worker_final.head(50))
    print("\n")

    #cgi用URL表示
    print("生産数量："+local_root_url+production_qty_data_agg_per_hour_dirname[1:] + "/" + production_qty_per_hour_output_file_name+"\n")
    print("電力量："+local_root_url+electric_power_data_agg_dirname[1:] + "/" + electric_power_output_file_name+"\n")

def correlation_analysis_per_hour():
    #-------------------------------------------------------------1時間毎相関分析-------------------------------------------------------------#

    # データファイル存在確認(2021/9/29 追加 菊地)⇒この中を変更した場合、"./OutputData/CorrelationAnalysis/per_hour"内のエクセルファイルを削除してください
    correlation_analysis_per_hour_dirname_list = os.listdir(correlation_analysis_dirname + "/per_hour")
    if len(correlation_analysis_per_hour_dirname_list) == 0:
        #電力量分析ファイルパス
        electric_power_analysis_file_path = electric_power_data_agg_dirname + "/" + electric_power_analysis_file[0]
        #電力量分析ファイルをPandasで読み込み
        electric_power_per_hour_analysis_data = pd.read_excel(electric_power_analysis_file_path, sheet_name="electric_power",index_col=None) #2021/08/26 追記

        #1時間単位生産数量分析ファイルパス(2021/8/26 追加)
        production_quantity_per_hour_analysis_file_path = production_qty_data_agg_per_hour_dirname + "/" + production_qty_per_hour_analysis_files[0]
        #1時間単位生産数量分析ファイルをPandasで読み込み(2021/8/26 追加)
        production_quantity_per_hour_analysis_data = pd.read_excel(production_quantity_per_hour_analysis_file_path, index_col=None)

        #1時間単位相関分析
        df_per_hour_merge = ca.dataframe_join(electric_power_per_hour_analysis_data,production_quantity_per_hour_analysis_data)
        df_per_hour_basic_unit = ca.basic_unit_calculate(df_per_hour_merge)
        print("===========1時間単位相関分析===========\n")
        print(df_per_hour_basic_unit)

        #1時間単位State/Action + Number_of_setup_changes/Setupの判定(2021/9/1 追加、2021/9/7 Number_of_setup_changes 追加)
        df_per_hour_state_action = ca.calc_state(df_per_hour_basic_unit)

        #電力消費量ロス(Startup_loss/Down_loss/Setup_loss/Run_loss)の計算(2021/9/7 追加)
        df_power_loss_per_hour = ca.calc_loss(df_per_hour_state_action) 
#　ここまでで分析終わっている…？ここから再計算する意味がわからない…。

# 画像出力
        # データファイル存在確認(2021/9/29 追加 菊地)
        fig_scatterplot_basicunit_dirname_list = os.listdir(fig_scatterplot_basicunit_dirname)
        if len(fig_scatterplot_basicunit_dirname_list) == 0:
            #時系列(全期間)の電力量原単位グラフ出力 関数化(2021/9/29 追加 菊地)
            ca.make_figure_scatterplot_basicunit(df_power_loss_per_hour)
        else:
            print("=======figure_scatterplot_basicunit_Pass=============\n")
            pass

        # データファイル存在確認(2021/9/29 追加 菊地)
        fig_time_basicunit_dirname_list = os.listdir(fig_time_basicunit_dirname)
        if len(fig_time_basicunit_dirname_list) == 0:
            #時系列(12か月×3通り(B/M/E))の電力量原単位グラフ出力(2021/9/22 追加)
            for month in range(1, 13):
                ca.make_figure_per_month(df_power_loss_per_hour,month)
        else:
            print("=======figure_time_basicunit_Pass=============\n")
            pass

        # データファイル存在確認(2021/9/29 追加 菊地)
        fig_datacount_basicunit_dirname_list = os.listdir(fig_datacount_basicunit_dirname)
        if len(fig_datacount_basicunit_dirname_list) == 0:
            #電力量原単位ヒストグラム出力(2021/9/22 追加)
            ca.make_figure_data_count(df_power_loss_per_hour)
        else:
            print("=======figure_data_count_Pass=============\n")
            pass
# 画像出力終わり


# ここから何しているのかわからない。金型毎アクション値毎に
        #gropby前加工(金型毎Action毎作成用)(2021/9/21 追加、(2021/9/27 df_pre_groupby3の条件変更)
        df_pre_groupby1 = df_power_loss_per_hour[df_power_loss_per_hour['Setup_flag'] != 1].copy()
        df_pre_groupby2 = df_pre_groupby1[(df_pre_groupby1['Action'] == 4) | (df_pre_groupby1['Action'] == 8) | (df_pre_groupby1['Action'] == 12)].copy()
        df_pre_groupby3 = df_power_loss_per_hour[(df_power_loss_per_hour['Setup_flag'] == 1) & (df_power_loss_per_hour['Action'] == 4) | (df_power_loss_per_hour['Action'] == 8) | (df_power_loss_per_hour['Action'] == 12)].copy()
        df_pre_groupby4 = df_power_loss_per_hour[(df_power_loss_per_hour['Action'] == 4) | (df_power_loss_per_hour['Action'] == 8) | (df_power_loss_per_hour['Action'] == 12)].copy()

        # データファイル存在確認(2021/9/30 追加 菊地)
        correlation_analysis_basicunit_per_mold_dirname_list = os.listdir(correlation_analysis_dirname + "/basicunit_per_mold")
        if len(correlation_analysis_basicunit_per_mold_dirname_list) == 0:
            #gropbyによる金型毎df作成(2021/9/21 追加)
            df_basicunit_per_mold = ca.calculate_basicunit_per_mold(df_pre_groupby2)

            #金型単位の原単位ファイル保存設定(2021/9/21 追加)
            power_consumption_basic_unit_per_mold_file_name ='[Confidential]Power_Consumption_Basic_Unit_per_Mold.xlsx'
            power_consumption_basic_unit_per_mold_file = pd.ExcelWriter(correlation_analysis_dirname + "/basicunit_per_mold/" + power_consumption_basic_unit_per_mold_file_name)
            df_basicunit_per_mold.to_excel(power_consumption_basic_unit_per_mold_file, "per_mold(BasicUnit)", index = False)
            
            #金型単位の原単位ファイル保存(2021/9/21 追加)
            power_consumption_basic_unit_per_mold_file.save()
        else:
            print("=======correlation_analysis_basicunit_per_mold_Pass=============\n")
            pass

        # データファイル存在確認(2021/9/30 追加 菊地)
        correlation_analysis_basicunit_per_action_dirname_list = os.listdir(correlation_analysis_dirname + "/basicunit_per_action")
        if len(correlation_analysis_basicunit_per_action_dirname_list) == 0:
            #gropbyによるaction毎df作成(2021/9/21 追加)
            df_basicunit_per_action = ca.calculate_basicunit_per_action(df_pre_groupby1)

            #Action単位の原単位ファイル保存設定(2021/9/21 追加)
            power_consumption_basic_unit_per_action_file_name ='[Confidential]Power_Consumption_Basic_Unit_per_Action.xlsx'
            power_consumption_basic_unit_per_action_file = pd.ExcelWriter(correlation_analysis_dirname + "/basicunit_per_action/" + power_consumption_basic_unit_per_action_file_name)
            df_basicunit_per_action.to_excel(power_consumption_basic_unit_per_action_file, "per_action(BasicUnit)", index = False)

            #gropbyによるsetupのdf作成(2021/9/21 追加)
            df_basicunit_per_action2 = ca.calculate_basicunit_per_setup(df_pre_groupby3)

            #Action単位の原単位ファイル保存設定(2021/9/21 追加)
            df_basicunit_per_action2.to_excel(power_consumption_basic_unit_per_action_file, "setup(BasicUnit)", index = False)

            #Action単位の原単位ファイル保存(2021/9/21 追加)
            power_consumption_basic_unit_per_action_file.save()
        else:
            print("=======correlation_analysis_basicunit_per_action_Pass=============\n")
            pass

        # データファイル存在確認(2021/9/30 追加 菊地)
        correlation_analysis_basicunit_per_mold_action_dirname_list = os.listdir(correlation_analysis_dirname + "/basicunit_per_mold_action")
        if len(correlation_analysis_basicunit_per_mold_action_dirname_list) == 0:
            #gropbyによる金型毎action毎df作成(2021/9/21 追加)
            df_basicunit_per_mold_action5 = ca.calculate_basicunit_per_mold_action(df_pre_groupby2)

            #金型毎Action毎の原単位ファイル保存設定(2021/9/21 追加)
            power_consumption_basic_unit_per_mold_action_file_name ='[Confidential]Power_Consumption_Basic_Unit_per_Mold_Action.xlsx'
            power_consumption_basic_unit_per_mold_action_file = pd.ExcelWriter(correlation_analysis_dirname + "/basicunit_per_mold_action/" + power_consumption_basic_unit_per_mold_action_file_name)
            df_basicunit_per_mold_action5.to_excel(power_consumption_basic_unit_per_mold_action_file, "per_mold_action(BasicUnit)", index = False)

            #gropbyによる金型毎action毎powerのdf作成(2021/9/28 追加)
            df_power_per_mold_action = ca.calculate_power_per_mold_action(df_pre_groupby1)

            #金型毎Action単位の電力量ファイル保存設定(2021/9/28 追加)
            df_power_per_mold_action.to_excel(power_consumption_basic_unit_per_mold_action_file, "per_mold_action(Power)", index = False)

            #gropbyによるsetupの金型毎df作成(2021/9/21 追加)
            df_basicunit_per_mold_action5_2 = ca.calculate_basicunit_per_mold_setup(df_pre_groupby3)

            #金型毎Action毎の原単位ファイル保存設定(2021/9/21 追加)
            df_basicunit_per_mold_action5_2.to_excel(power_consumption_basic_unit_per_mold_action_file, "setup_per_mold(BasicUnit)", index = False)
            
            #金型毎Action毎の原単位ファイル保存(2021/9/21 追加)
            power_consumption_basic_unit_per_mold_action_file.save()
        else:
            print("=======correlation_analysis_basicunit_per_mold_action_Pass=============\n")
            pass

        #金型毎Action毎の原単位ファイルパス(2021/9/30 追加)
        power_consumption_basic_unit_per_mold_action_file_name ='[Confidential]Power_Consumption_Basic_Unit_per_Mold_Action.xlsx'
        power_consumption_basic_unit_per_mold_action_file = correlation_analysis_dirname + "/basicunit_per_mold_action/" + power_consumption_basic_unit_per_mold_action_file_name
#これらのファイルがなんなのかがわからない…5と5_2が何を意味している…？        
        #金型毎Action毎の原単位ファイルをPandasで呼び出し(2021/9/30 追加)
        df_basicunit_per_mold_action5 = pd.read_excel(power_consumption_basic_unit_per_mold_action_file, index_col=None, sheet_name="per_mold_action(BasicUnit)")
        df_basicunit_per_mold_action5_2 = pd.read_excel(power_consumption_basic_unit_per_mold_action_file, index_col=None, sheet_name="setup_per_mold(BasicUnit)")
        df_power_per_mold_action = pd.read_excel(power_consumption_basic_unit_per_mold_action_file, index_col=None, sheet_name="per_mold_action(Power)")

# この辺全てrunlossになってる…？それぞれのlossがどれ（立ち上がりとか）に対応しているのかよくわからない。
        #Run_loss再計算(2021/9/22 追加)
        df_modified_runloss_per_hour = ca.runloss_calculate_per_mold_action(df_basicunit_per_mold_action5,df_power_loss_per_hour)

        #Setup_loss再計算(2021/9/22 追加)
        df_modified_runloss_per_hour = ca.setuploss_calculate_per_mold_action(df_basicunit_per_mold_action5_2,df_modified_runloss_per_hour)

        #Startup_loss再計算(2021/9/22 追加)
        df_modified_runloss_per_hour = ca.startuploss_calculate_per_mold_action(df_power_per_mold_action,df_modified_runloss_per_hour)

        #Down_loss再計算(2021/9/22 追加)
        df_modified_runloss_per_hour = ca.downloss_calculate_per_mold_action(df_power_per_mold_action,df_modified_runloss_per_hour)

        #1時間単位最終ファイル保存設定(2021/9/22 追加)
        correlation_analysis_per_hour_file_name ='[Confidential]Correlation_Analysis_per_hour.xlsx'
        correlation_analysis_per_hour_output_file = pd.ExcelWriter(correlation_analysis_dirname + "/per_hour/" + correlation_analysis_per_hour_file_name)
        df_modified_runloss_per_hour.to_excel(correlation_analysis_per_hour_output_file, "per_hour", index = False)
        
        #1時間単位最終ファイル保存(2021/9/22 追加)
        correlation_analysis_per_hour_output_file.save()
    else:
        print("=======correlation_analysis_per_hour_Pass=============\n")
        pass

    # # データファイル存在確認(2021/9/30 追加 菊地)
    # correlation_analysis_per_hour_dirname_list = os.listdir(correlation_analysis_dirname + "/per_hour")

    #cgi用URL表示
    correlation_analysis_per_hour_file_name ='[Confidential]Correlation_Analysis_per_hour.xlsx'
    print("時間毎相関分析結果："+local_root_url+correlation_analysis_dirname[1:] + "/per_hour/" + correlation_analysis_per_hour_file_name+"\n")
    print("可視化データ："+local_root_url+fig_time_basicunit_dirname[1:] +"\n")

def correlation_analysis_per_day():
    #-------------------------------------------------------------日毎相関分析-------------------------------------------------------------#

    # データファイル存在確認(2021/9/29 追加 菊地)⇒この中を変更した場合、"./OutputData/CorrelationAnalysis/per_day"内のエクセルファイルを削除してください
    correlation_analysis_per_day_dirname_list = os.listdir(correlation_analysis_dirname + "/per_day")
    if len(correlation_analysis_per_day_dirname_list) == 0:
        #1時間単位最終ファイルパス(2021/9/30 追加)
        correlation_analysis_per_hour_file_name ='[Confidential]Correlation_Analysis_per_hour.xlsx'
        correlation_analysis_per_hour_output_file = correlation_analysis_dirname + "/per_hour/" + correlation_analysis_per_hour_file_name
        #1時間単位最終ファイルをPandasで呼び出し(2021/9/30 追加)
        df_modified_runloss_per_hour = pd.read_excel(correlation_analysis_per_hour_output_file, index_col=None)

        #1日単位で~_loss_timeを出力するための機構(2021/9/7 追加)
        df_loss_time_per_hour = ca.data_per_hour_add_loss_time(df_modified_runloss_per_hour).copy()

        #各種統計量計算(2021/9/7 追加)
        df_power_calculate_for_analysis_per_day = ca.power_per_day_calculate_for_analysis(df_loss_time_per_hour).copy()
        
        # Setup_timeのためのデータ加工(2021/9/9 追加)
        df_per_hour_for_resampling = df_loss_time_per_hour.copy()
        df_per_hour_for_resampling.set_axis(pd.to_datetime(df_per_hour_for_resampling['DateTime']), axis='index', inplace=True)
        df_per_hour_for_resampling = df_per_hour_for_resampling.drop(columns =['DateTime'])
        
        # 生産数量1h単位リサンプル前ファイルデータ読み込み(2021/9/6 追加)
        production_qty_per_hour_not_yet_resample_file = os.listdir(production_qty_data_agg_per_hour_not_yet_resample_dirname)

        # 生産数量1h単位リサンプル前ファイルデータパス(2021/9/6 追加)
        production_quantity_per_hour_not_yet_resample_file_path = production_qty_data_agg_per_hour_not_yet_resample_dirname + "/" + production_qty_per_hour_not_yet_resample_file[0]
        # 生産数量1h単位リサンプル前ファイルデータをPandasで読み込み(2021/9/6 追加)
        df_not_yet_resumple = pd.read_excel(production_quantity_per_hour_not_yet_resample_file_path, index_col=None)

        # 生産数量1h単位リサンプル前ファイルデータを使用するためのデータ加工(2021/9/6 追加)
        df_not_yet_resumple.set_axis(pd.to_datetime(df_not_yet_resumple['DateTime']), axis='index', inplace=True)
        df_not_yet_resumple = df_not_yet_resumple.drop(columns =['DateTime'])

        #Setup_time出力用計算(2021/9/9 追加)
        df_setup_time = ca.setup_time_calculate(df_per_hour_for_resampling,df_not_yet_resumple)

        #1日単位相関分析(Mold_typeによる分類)(2021/9/7 追加、2021/9/9 編集)
        df_per_day_mold_pre_merge = ca.data_agg_per_day_mold(df_loss_time_per_hour,df_setup_time).copy()
        print("==============df_per_day_mold_pre_merge==============\n")
        print(df_per_day_mold_pre_merge)

        #原単位再計算用加工(関数化 2021/9/13)
        df_per_day_mold_for_basic_unit,s_int_day,original_index_list_day = ca.data_processing_for_basic_unit(df_per_day_mold_pre_merge)

        #原単位再計算(1日毎金型毎)
        df_per_day_mold_basic_unit = ca.basic_unit_calculate(df_per_day_mold_for_basic_unit)

        #マージ(1日毎金型毎)(2021/9/13 関数化)
        df_per_day_mold = ca.merge_for_per_mold(df_per_day_mold_pre_merge,df_per_day_mold_basic_unit,s_int_day,original_index_list_day)

        #1日単位相関分析(Mold_typeによる分類)ファイル保存設定　2021/9/6追加、2021/9/9 編集
        correlation_analysis_per_day_mold_file_name ='[Confidential]Correlation_Analysis_per_day_mold.xlsx'
        correlation_analysis_per_day_mold_output_file = pd.ExcelWriter(correlation_analysis_dirname + "/per_day_mold/" + correlation_analysis_per_day_mold_file_name)
        df_per_day_mold.to_excel(correlation_analysis_per_day_mold_output_file, "correlation_analysis_mold", index = False)
        
        #1日単位相関分析(Mold_typeによる分類)ファイル保存(2021/9/9 追加)
        correlation_analysis_per_day_mold_output_file.save()

        #1日単位に1Mold単位の情報を内包したdf(2021/9/7 追加)
        df_per_day_mold_order_pre_drop,df_per_day_mold_order = pa.product_type_classify(df_per_day_mold)

        #原単位再計算(1日)用データ加工⇒後で関数の中に入れる
        df_per_hour_basic_unit_pre_resumple = df_loss_time_per_hour[['DateTime','Shot_count','PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost']].copy()
        df_per_hour_basic_unit_pre_resumple.set_axis(pd.to_datetime(df_per_hour_basic_unit_pre_resumple['DateTime']), axis='index', inplace=True)
        df_per_hour_basic_unit_pre_resumple = df_per_hour_basic_unit_pre_resumple.drop(columns =['DateTime'])
        df_per_hour_basic_unit_resumpled = df_per_hour_basic_unit_pre_resumple.resample('1D').sum().copy()

        #原単位再計算(1日)
        df_per_day_basic_unit = ca.basic_unit_calculate(df_per_hour_basic_unit_resumpled)

        #金型列の消去+1日再リサンプル(2021/9/7 追加、2021/9/9 編集)
        df_per_day_pre_merge1,df_per_day_pre_merge2 = ca.mold_drop_and_resample_per_day(df_loss_time_per_hour,df_setup_time)

        #1日金型結果⇒1日結果とするためのマージ(2021/9/7 追加 2021/9/13 関数化)
        df_per_day_merged = ca.mold_to_not_mold_per_day(df_per_day_basic_unit,df_per_day_pre_merge1,df_per_day_pre_merge2,df_per_day_mold_order,df_power_calculate_for_analysis_per_day)

        #1日単位相関分析ファイル保存設定(2021/9/6 追加)
        correlation_analysis_per_day_file_name ='[Confidential]Correlation_Analysis_per_day.xlsx'
        correlation_analysis_per_day_output_file = pd.ExcelWriter(correlation_analysis_dirname + "/per_day/" + correlation_analysis_per_day_file_name)
        df_per_day_merged.to_excel(correlation_analysis_per_day_output_file, "correlation_analysis", index = False)
        
        #1日単位相関分析ファイル保存(2021/9/6 追加)
        correlation_analysis_per_day_output_file.save()
    else:
        print("=======correlation_analysis_per_day_Pass=============\n")
        pass

    #1日単位金型毎ファイルパス(2021/9/30 追加)
    correlation_analysis_per_day_mold_file_name ='[Confidential]Correlation_Analysis_per_day_mold.xlsx'
    correlation_analysis_per_day_mold_output_file = correlation_analysis_dirname + "/per_day_mold/" + correlation_analysis_per_day_mold_file_name
    #1日単位金型毎ファイルをPandasで呼び出し(2021/9/30 追加)
    df_per_day_mold = pd.read_excel(correlation_analysis_per_day_mold_output_file, index_col=None)

    #1日単位相関分析ファイルパス(2021/9/30 追加)
    correlation_analysis_per_day_file_name ='[Confidential]Correlation_Analysis_per_day.xlsx'
    correlation_analysis_per_day_output_file = correlation_analysis_dirname + "/per_day/" + correlation_analysis_per_day_file_name
    #1日単位相関分析ファイルをPandasで呼び出し(2021/9/30 追加)
    df_per_day_merged = pd.read_excel(correlation_analysis_per_day_output_file, index_col=None)

    return df_per_day_mold, df_per_day_merged

def correlation_analysis_per_month(df_per_day_mold, df_per_day_merged):
    #-------------------------------------------------------------月毎相関分析-------------------------------------------------------------#

    # データファイル存在確認(2021/9/29 追加 菊地)⇒この中を変更した場合、"./OutputData/CorrelationAnalysis/per_month"内のエクセルファイルを削除してください
    correlation_analysis_per_month_dirname_list = os.listdir(correlation_analysis_dirname + "/per_month")
    if len(correlation_analysis_per_month_dirname_list) == 0:

        #1か月単位金型単位計算(2021/9/13 追加)
        df_per_month_mold_pre_merge = ca.data_agg_per_month_mold(df_per_day_mold)

        #原単位再計算用加工(関数化 2021/9/13)
        df_per_month_mold_for_basic_unit,s_int_month,original_index_list_month = ca.data_processing_for_basic_unit(df_per_month_mold_pre_merge)

        #原単位再計算(1か月毎金型毎)
        df_per_month_mold_basic_unit = ca.basic_unit_calculate(df_per_month_mold_for_basic_unit)

        #マージ(1か月毎金型毎)(2021/9/13 関数化)
        df_per_month_mold = ca.merge_for_per_mold(df_per_month_mold_pre_merge,df_per_month_mold_basic_unit,s_int_month,original_index_list_month)

        #原単位統計量計算(2021/9/14 追加)
        df_per_month_mold2 = ca.data_agg_per_month_mold_basic_unit(df_per_day_mold)
        df_per_month_mold3 = pd.merge(df_per_month_mold,df_per_month_mold2,on=['DateTime','Mold_type'],how="outer").copy()

        #1か月単位相関分析(Mold_typeによる分類)ファイル保存設定　2021/9/13追加
        correlation_analysis_per_month_mold_file_name ='[Confidential]Correlation_Analysis_per_month_mold.xlsx'
        correlation_analysis_per_month_mold_output_file = pd.ExcelWriter(correlation_analysis_dirname + "/per_month_mold/" + correlation_analysis_per_month_mold_file_name)
        df_per_month_mold3.to_excel(correlation_analysis_per_month_mold_output_file, "correlation_analysis_mold", index = False)
        
        #1か月単位相関分析(Mold_typeによる分類)ファイル保存(2021/9/13 追加)
        correlation_analysis_per_month_mold_output_file.save()

        #1日⇒1か月リサンプル(Mold_typeによる分類ではない)(2021/9/13 追加)
        df_per_day_merged.set_index("DateTime", inplace = True)
        df_per_month_resampled = df_per_day_merged.resample('1MS').sum().copy()
        print(df_per_month_resampled)

        #原単位再計算(1日)用データ加工(2021/9/13 追加)
        df_per_month_basic_unit_resumpled = df_per_month_resampled[['Shot_count','PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost']].copy()

        #原単位再計算(1か月)(2021/9/13 追加)
        df_per_month_basic_unit = ca.basic_unit_calculate(df_per_month_basic_unit_resumpled)

        #金型列の消去+1か月再リサンプル(2021/9/13 追加)
        df_per_month_pre_merge1,df_per_month_pre_merge2 = ca.mold_drop_and_resample_per_month(df_per_day_merged)

        #各種統計量計算(2021/9/13 追加)
        df_power_calculate_for_analysis_per_month = ca.power_per_month_calculate_for_analysis(df_per_day_merged).copy()

        #1か月金型結果⇒1か月結果とするためのマージ(2021/9/7 追加 2021/9/13 関数化)
        df_per_month_merged = ca.mold_to_not_mold(df_per_month_basic_unit,df_per_month_pre_merge1,df_per_month_pre_merge2,df_power_calculate_for_analysis_per_month)

        #1か月単位相関分析ファイル保存設定(2021/9/6 追加)
        correlation_analysis_per_month_file_name ='[Confidential]Correlation_Analysis_per_month.xlsx'
        correlation_analysis_per_month_output_file = pd.ExcelWriter(correlation_analysis_dirname + "/per_month/" + correlation_analysis_per_month_file_name)
        df_per_month_merged.to_excel(correlation_analysis_per_month_output_file, "correlation_analysis", index = False)
        
        #1か月単位相関分析ファイル保存(2021/9/6 追加)
        correlation_analysis_per_month_output_file.save()
    else:
        print("=======correlation_analysis_per_month_Pass=============\n")
        pass

    #1月単位金型毎ファイルパス
    correlation_analysis_per_month_mold_file_name ='[Confidential]Correlation_Analysis_per_month_mold.xlsx'
    correlation_analysis_per_month_mold_output_file = correlation_analysis_dirname + "/per_month_mold/" + correlation_analysis_per_month_mold_file_name
    #1月単位金型毎ファイルをPandasで呼び出し
    df_per_month_mold = pd.read_excel(correlation_analysis_per_month_mold_output_file, index_col=None)

    #1月単位相関分析ファイルパス
    correlation_analysis_per_month_file_name ='[Confidential]Correlation_Analysis_per_month.xlsx'
    correlation_analysis_per_month_output_file = correlation_analysis_dirname + "/per_month/" + correlation_analysis_per_month_file_name
    #1月単位相関分析ファイルをPandasで呼び出し
    df_per_month_merged = pd.read_excel(correlation_analysis_per_month_output_file, index_col=None)

    return df_per_month_mold, df_per_month_merged

def correlation_analysis_per_year(df_per_day_mold, df_per_day_merged, df_per_month_mold, df_per_month_merged):
    #-------------------------------------------------------------全期間（一年）相関分析-------------------------------------------------------------#

    # データファイル存在確認(2021/9/29 追加 菊地)⇒この中を変更した場合、"./OutputData/CorrelationAnalysis/per_year"内のエクセルファイルを削除してください
    correlation_analysis_per_year_dirname_list = os.listdir(correlation_analysis_dirname + "/per_year")
    if len(correlation_analysis_per_year_dirname_list) == 0:

        #1年単位金型単位計算(2021/9/13 追加)
        df_per_year_mold_pre_merge = ca.data_agg_per_year_mold(df_per_month_mold)

        #原単位再計算用加工(関数化 2021/9/13)
        df_per_year_mold_for_basic_unit,s_int_year,original_index_list_year = ca.data_processing_for_basic_unit(df_per_year_mold_pre_merge)

        #原単位再計算(1年毎金型毎)(2021/9/13 追加)
        df_per_year_mold_basic_unit = ca.basic_unit_calculate(df_per_year_mold_for_basic_unit)

        #マージ(1年毎金型毎)(2021/9/13 関数化)
        df_per_year_mold = ca.merge_for_per_mold(df_per_year_mold_pre_merge,df_per_year_mold_basic_unit,s_int_year,original_index_list_year)

        #原単位統計量計算(2021/9/14 追加)
        df_per_year_mold2 = ca.data_agg_per_year_mold_basic_unit(df_per_month_mold)
        df_per_year_mold3 = pd.merge(df_per_year_mold,df_per_year_mold2,on=['DateTime','Mold_type'],how="outer").copy()    

        #出力用設定(2021/9/14 追加)
        df_per_year_mold_output = ca.df_per_year_mold_output_setting(df_per_year_mold3)

        #1年単位相関分析(Mold_typeによる分類)ファイル保存設定　2021/9/13追加
        correlation_analysis_per_year_mold_file_name ='[Confidential]Correlation_Analysis_per_year_mold.xlsx'
        correlation_analysis_per_year_mold_output_file = pd.ExcelWriter(correlation_analysis_dirname + "/per_year_mold/" + correlation_analysis_per_year_mold_file_name)
        df_per_year_mold_output.to_excel(correlation_analysis_per_year_mold_output_file, "correlation_analysis_mold", index = False)
        
        #1年単位相関分析(Mold_typeによる分類)ファイル保存(2021/9/13 追加)
        correlation_analysis_per_year_mold_output_file.save()

        #1か月⇒1年リサンプル(Mold_typeによる分類ではない)(2021/9/14 追加)
        df_per_month_merged.set_index("DateTime", inplace = True)
        df_per_month_merged = df_per_month_merged.shift(-3,freq="M").copy()
        df_per_year_resampled = df_per_month_merged.resample('1AS').sum().copy()
        print(df_per_year_resampled)

        #原単位再計算(1年)用データ加工(2021/9/14 追加)
        df_per_year_basic_unit_resumpled = df_per_year_resampled[['Shot_count','PowerConsumption','CO2emissions','ElectricPowerCost','CO2emissionsCost']].copy()

        #原単位再計算(1年)(2021/9/14 追加)
        df_per_year_basic_unit = ca.basic_unit_calculate(df_per_year_basic_unit_resumpled)

        #金型列の消去+1年再リサンプル(2021/9/14 追加)
        df_per_year_pre_merge1,df_per_year_pre_merge2 = ca.mold_drop_and_resample_per_year(df_per_month_merged)

        #各種統計量計算(2021/9/14 追加)
        df_power_calculate_for_analysis_per_year = ca.power_per_year_calculate_for_analysis(df_per_month_merged).copy()

        #1か月結果⇒1年結果とするためのマージ(2021/9/7 追加 2021/9/13 関数化)
        df_per_year_merged = ca.mold_to_not_mold(df_per_year_basic_unit,df_per_year_pre_merge1,df_per_year_pre_merge2,df_power_calculate_for_analysis_per_year)

        #出力用設定(2021/9/14 追加)
        df_per_year_merged_output = ca.df_per_year_mold_output_setting(df_per_year_merged)

        #1年単位相関分析ファイル保存設定(2021/9/14 追加)
        correlation_analysis_per_year_file_name ='[Confidential]Correlation_Analysis_per_year.xlsx'
        correlation_analysis_per_year_output_file = pd.ExcelWriter(correlation_analysis_dirname + "/per_year/" + correlation_analysis_per_year_file_name)
        df_per_year_merged_output.to_excel(correlation_analysis_per_year_output_file, "correlation_analysis", index = False)
        
        #1年単位相関分析ファイル保存(2021/9/14 追加)
        correlation_analysis_per_year_output_file.save()

        # データファイル存在確認(2021/9/29 追加 菊地)
        fig_barplot_dirname_list = os.listdir(fig_barplot_dirname)
        if len(fig_barplot_dirname_list) == 0:
            #棒グラフ出力(2021/9/29 関数化 菊地)
            ca.make_figure_barplot(df_per_year_mold_output)
        else:
            print("=======figure_barplot_Pass=============\n")
            pass
    else:
        print("=======correlation_analysis_per_year_Pass=============\n")
        pass

    #cgi用URL表示
    correlation_analysis_per_year_file_name ='[Confidential]Correlation_Analysis_per_year.xlsx'
    print("年間相関分析結果："+local_root_url+correlation_analysis_dirname[1:] + "/per_year/" + correlation_analysis_per_year_file_name+"\n")

    # データファイル存在確認(2021/9/29 追加 菊地)
    fig_violinplot_dirname_list = os.listdir(fig_violinplot_dirname)
    if len(fig_violinplot_dirname_list) == 0:
        #ヴァイオリンプロット出力(2021/9/29 関数化 菊地)
        ca.make_figure_violinplot(df_per_day_mold)
    else:
        print("=======figure_violinplot_Pass=============\n")
        pass

    # データファイル存在確認(2021/9/29 追加 菊地)
    fig_scatterplot_shotcount_power_dirname_list = os.listdir(fig_scatterplot_shotcount_power_dirname)
    if len(fig_scatterplot_shotcount_power_dirname_list) == 0:
        #散布図出力 関数化(2021/9/29 菊地)
        ca.make_figure_scatterplot_shotcount_power(df_per_day_merged)
    else:
        print("=======figure_scatterplot_shotcount_power_Pass=============\n")
        pass

    # データファイル存在確認(2021/9/29 追加 菊地)
    fig_scatterplot_shotcount_power_by_mold_dirname_list = os.listdir(fig_scatterplot_shotcount_power_by_mold_dirname)
    if len(fig_scatterplot_shotcount_power_by_mold_dirname_list) == 0:
        #散布図出力 関数化(2021/9/29 菊地)
        ca.make_figure_scatterplot_shotcount_power_by_mold(df_per_day_mold)
    else:
        print("=======figure_scatterplot_shotcount_power_by_mold_Pass=============\n")
        pass

def multiple_regression_analysis(df_per_day_merged):
    #-------------------------------------------------------------重回帰分析-------------------------------------------------------------#

	# #重回帰分析(古い)
    # df_explanatory_var,df_objective_var = ma.analytical_preparation(electric_multi_regression_analysis_data, production_multi_regression_analysis_data)
    # multiple_regression_analysis, explanatory_var,objective_var, result  = ma.multiple_regression_analysis(df_explanatory_var, df_objective_var)
    # ma.multiple_regression_analysis_visual(explanatory_var,objective_var, result)

    #重回帰分析(最新)⇒重回帰分析するには./OutputData/MultipleRegression/PreMultipleRegression/explanatory/のファイルを削除する
    index_for_multiple_regression_analysis=22
    df_per_day_merged1 = df_per_day_merged.iloc[0:,index_for_multiple_regression_analysis:index_for_multiple_regression_analysis+18].copy()
    print(df_per_day_merged1)

    # データファイル存在確認(2021/9/30 追加 菊地)⇒この中を動かすときは./OutputData/ProductionQuantity/ProductionQuantity_per_day/No24を削除
    production_qty_data_agg_per_day_dirname_list = os.listdir(production_qty_data_agg_per_day_dirname)
    if len(production_qty_data_agg_per_day_dirname_list) == 0:       
        df_production_analysis_par_day = df_per_day_merged1.copy()
        df_production_analysis_par_day.insert(loc=0, column='DateTime', value=df_production_analysis_par_day.index.values) 

        #1年単位相関分析ファイル保存設定(2021/9/6 追加)
        production_quantity_analysis_file_name_new ='[Confidential]Production_Quantity_per_day_Analysis.xlsx'
        production_quantity_analysis_file_new = pd.ExcelWriter(production_qty_data_agg_per_day_dirname + "/" + production_quantity_analysis_file_name_new)
        df_production_analysis_par_day.to_excel(production_quantity_analysis_file_new, "24SC_production_analysis", index = False)
        
        #1年単位相関分析ファイル保存(2021/9/6 追加)
        production_quantity_analysis_file_new.save()
    else:
        print("=======production_qty_data_agg_per_day_Pass=============\n")
        pass


    # データファイル存在確認(2021/9/30 追加 菊地)
    pre_multi_regression_analysis_dirname_list = os.listdir(pre_multi_regression_analysis_dirname)
    production_qty_data_agg_per_day_dirname_list = os.listdir(production_qty_data_agg_per_day_dirname)

    #電力量分析ファイルパス
    electric_power_analysis_file_path = electric_power_data_agg_dirname + "/" + electric_power_analysis_file[0]
    electric_multi_regression_analysis_data = pd.read_excel(electric_power_analysis_file_path, sheet_name="MultiRegressionAnalysis",index_col=None)
    if len(pre_multi_regression_analysis_dirname_list) == 0 and len(production_qty_data_agg_per_day_dirname_list) != 0:
        # 1次分析データ読み込み
        # electric_power_analysis_file = os.listdir(electric_power_data_agg_dirname)
        production_quantity_analysis_file_new = os.listdir(production_qty_data_agg_per_day_dirname)
        #生産数量分析ファイルパス
        production_quantity_analysis_file_path_new = production_qty_data_agg_per_day_dirname + "/" + production_quantity_analysis_file_new[0]
        #生産数量分析ファイルをPandasで読み込み
        production_multi_regression_analysis_data_new = pd.read_excel(production_quantity_analysis_file_path_new, sheet_name="24SC_production_analysis",index_col=None)
        df_explanatory_var_new,df_objective_var_new = ma.analytical_preparation(electric_multi_regression_analysis_data, production_multi_regression_analysis_data_new)
        #重回帰分析説明変数入力データファイル保存
        out_file_0e = pd.ExcelWriter(pre_multi_regression_analysis_dirname + "/" + "Pre_Multiple_Regression.xlsx")
        df_explanatory_var_new.to_excel(out_file_0e, "explanatory_var", index = False)
        out_file_0e.save()
        #重回帰分析目的変数入力データファイル保存
        out_file_0o = pd.ExcelWriter(pre_multi_regression_analysis_dirname + "/" + "Pre_Multiple_Regression.xlsx")
        df_objective_var_new.to_excel(out_file_0o, "objective_var", index = False)
        out_file_0o.save()

        multiple_regression_analysis_new, explanatory_var_new,objective_var_new, result_new  = ma.multiple_regression_analysis(df_explanatory_var_new, df_objective_var_new)
        # ma.multiple_regression_analysis_visual(multiple_regression_analysis_new,objective_var_new, result_new) #後で戻す
        #重回帰分析結果ファイル出力
        s = io.StringIO(multiple_regression_analysis_new.as_csv())
        with open('Multiple_Regression_Result_new.csv', 'w',encoding='utf-8') as f:
            for line in s:
                f.write(line)

        #重回帰分析説明変数データファイル出力
        out_file = pd.ExcelWriter(multi_regression_analysis_dirname + "/" + "Multiple_Regression_Explanatory_Objective_Var.xlsx")
        df_explanatory_objective_var_new = pd.concat([df_objective_var_new, df_explanatory_var_new], axis = 1)
        df_explanatory_objective_var_new.to_excel(out_file, "explanatory_objective_var", index = False)
        out_file.save()
    else:
        print("=======multiple_regression_analysis_Pass=============\n")
        pass

	# #重回帰分析説明変数データファイル出力(古い)
    # out_file = pd.ExcelWriter(multi_regression_analysis_dirname + "/" + "Multiple_Regression_Explanatory_Objective_Var.xlsx")
    # df_explanatory_objective_var = pd.concat([df_objective_var, df_explanatory_var], axis = 1)
    # df_explanatory_objective_var.to_excel(out_file, "explanatory_objective_var", index = False)
    # out_file.save()

	# #重回帰分析結果ファイル出力(古い)
    # s = io.StringIO(multiple_regression_analysis.as_csv())
    # with open('Multiple_Regression_Result.csv', 'w',encoding='utf-8') as f:
    #     for line in s:
    #         f.write(line)

def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    to_time_series_data()
    correlation_analysis_per_hour()
    df_per_day_mold, df_per_day_merged=correlation_analysis_per_day()
    df_per_month_mold, df_per_month_merged=correlation_analysis_per_month(df_per_day_mold, df_per_day_merged)
    correlation_analysis_per_year(df_per_day_mold, df_per_day_merged, df_per_month_mold, df_per_month_merged)
    multiple_regression_analysis(df_per_day_merged)

if __name__ == '__main__':
	main()