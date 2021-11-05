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

#---------------------------------------------------
# 　読み込んだ生産数量のDataFrame化
#---------------------------------------------------
def production_number_calculte(Production_data_sheet):
    print("==============Production_sheet===============")
    print(Production_data_sheet)
    print("==============Production_sheet.index===============")
    print(Production_data_sheet.index)
    
    #生産数量
    df_production_number = pd.DataFrame()
    df_production_number = Production_data_sheet.copy().fillna({'Number_of_stacked_cores': 1})
    #この後やること
    #DateTimeを1番左にずらす
    insert_column = df_production_number.pop('DateTime')
    df_production_number.insert(0,'DateTime',insert_column)
    #DateTimeで昇順にする？
    df_sorted_production_number = df_production_number.sort_values(['ProductionLine','DateTime'])
    return df_sorted_production_number

#---------------------------------------------------
#   日毎のライン毎/型式毎の合計数量を算出
#---------------------------------------------------
def production_number_data_sheet_create(output_file, df_production_number):
    counter = 0
    #df_production_number_per_line = pd.DataFrame()
    for production_line,production_number in df_production_number.groupby('ProductionLine'):
        counter = counter + 1
        #df_production_number_per_line = pd.DataFrame()
        print("================groupby===============")
        print(production_line)
        print("================groupby===============")        
        print(production_number)
        exec(f"df_production_number_per_line{counter} = pd.DataFrame()") #DataFrameの番号を変数化(番号を可変させる)
        exec(f"df_production_number_per_line{counter} = production_number.copy()")
        exec(f"df_production_number_per_line{counter}.to_excel(output_file, production_line, index = False)") #Excelへ出力
        print("================df_production_number_per_line================")
        print(f"df_production_number_per_line{counter}")
        if production_line == str("24SC"):
            target_production_line = production_line
            df_target_production = production_number.copy()
    return target_production_line, df_target_production

#---------------------------------------------------
#   機種/金型毎の生産数量/段取り替え回数/ロット数を算出(2021/9/8 再編集)
#---------------------------------------------------
def product_type_classify(df_production_number):
    # データフレームの列名判定追加 2021/9/8
    dflist_columns = df_production_number.columns.values 
    if 'Product' in dflist_columns:
        product_type_list = df_production_number['Product'].unique()
    elif 'Mold_type' in dflist_columns:
        product_type_list = df_production_number['Mold_type'].unique()
    else:
        print("There is not df_columns...?")
    # product_type_list = df_production_number['Product'].unique()
    print("================product_type_list ===================")
    print(product_type_list)
    print("================len(product_type_list)===================")
    print(len(product_type_list))
    
    # 格納するDataFrameを作成
    #column_name_list = []
    column_name_list = ['DateTime']
    column_name_list.extend(product_type_list)
    df_production_agg = pd.DataFrame(columns = column_name_list)
    #df_production_agg = pd.DataFrame(columns = product_type_list)
    #df_production_agg.insert(1,columns = product_type_list)
    #print("==================df_production_agg==================")
    #print(df_production_agg)

    #同日の生産機種(金型)を抜き出す
    #df_production_number['DateTime']= df_production_number['DateTime'].dt.strftime("%Y/%m/%d").copy()
    print("================df_production_number===================")
    print(df_production_number)
    df_production_number['DateTime'] = df_production_number['DateTime'].dt.strftime("%Y/%m/%d").copy()
    df_production_number['DateTime'] = pd.to_datetime(df_production_number['DateTime'], format="%Y/%m/%d").copy()
    #df_production_number['DateTime'].dt.datetime.strptime(datetime_string, '%Y年%m月%d日 %H:%M:%S')
    #dt.datetime.strptime(datetime_string, '%Y年%m月%d日 %H:%M:%S') 
    production_order_list = [0] * 12 #初期化0の要素数12のリスト(1日のロット切り替えが最大12回であったため)
    df_production_order = pd.DataFrame(columns = ['DateTime','Order1_Product','Order2_Product','Order3_Product','Order4_Product','Order5_Product','Order6_Product','Order7_Product','Order8_Product','Order9_Product','Order10_Product','Order11_Product','Order12_Product'])
    order_length = 0
    max_order_length = 0
    df_concat = pd.DataFrame()
    df_merge =  pd.DataFrame()
    df_production_per_product_concat = pd.DataFrame()
    df_production_order_concat = pd.DataFrame()
    last_production_order = str("")
    # df_production_number.set_index("DateTime", inplace = True)
    for date, df_production in df_production_number.groupby('DateTime'):
        #コメント化 2021/9/8
        # print("===============date==============")
        # print(date)
        # print("===============df_production==============")
        # print(df_production)

        # データフレームの列名判定追加 2021/9/8
        if 'Product' in dflist_columns:
            #機種毎の生産数量合計計算
            df_production_per_product = df_production.groupby(['DateTime','Product','ProductionLine','ProductType', 'Number_of_stacked_cores']).sum().reset_index().copy()
            df_production_per_product.drop(columns=['ProductType', 'Number_of_stacked_cores','ProductionQuantity'])
            df_production_per_product['Product'] = str("ProductionQty_") + df_production_per_product['Product']
            df_production_per_product['Product'] = df_production_per_product['Product'].str.replace(' ', '') #2021/8/25 追記(重回帰分析計算の説明変数名に空白があるとエラーメッセージが出るため)
            df_production_per_product_pivot = df_production_per_product.pivot(index=['DateTime','ProductionLine'], columns='Product', values='Number_of_Production').reset_index().copy() #['DateTime','ProductionLine']の列は固定して、行と列を入れ替え
            #df_production_per_product_pivot.insert(2,"ProductionQuantity",df_production['Number_of_Production'].sum().copy())#1日の生産数量の合計値(全機種対象)を挿入
            print("===============機種毎の生産数量合計===============")
            print(df_production_per_product_pivot)
        elif 'Mold_type' in dflist_columns:
            #金型毎の生産数量合計計算
            df_production_per_product = df_production.groupby(['DateTime','Mold_type','ProductionLine']).sum().reset_index().copy()
            df_production_per_product.drop(columns=['Shot_count'])
            df_production_per_product['Mold_type'] = str("Shot_count_") + df_production_per_product['Mold_type']
            df_production_per_product['Mold_type'] = df_production_per_product['Mold_type'].str.replace(' ', '') #2021/8/25 追記(重回帰分析計算の説明変数名に空白があるとエラーメッセージが出るため)
            df_production_per_product_pivot = df_production_per_product.pivot(index=['DateTime','ProductionLine'], columns='Mold_type', values='Shot_count').reset_index().copy() #['DateTime','ProductionLine']の列は固定して、行と列を入れ替え
            #df_production_per_product_pivot.insert(2,"ProductionQuantity",df_production['Number_of_Production'].sum().copy())#1日の生産数量の合計値(全機種対象)を挿入
            #コメント化 2021/9/8            
            # print("===============金型毎の生産数量合計===============")
            # print(df_production_per_product_pivot)
        else:
            print("There is not df_columns...?")

        # #機種毎の生産数量合計計算
        # df_production_per_product = df_production.groupby(['DateTime','Product','ProductionLine','ProductType', 'Number_of_stacked_cores']).sum().reset_index().copy()
        # df_production_per_product.drop(columns=['ProductType', 'Number_of_stacked_cores','ProductionQuantity'])
        # df_production_per_product['Product'] = str("ProductionQty_") + df_production_per_product['Product']
        # df_production_per_product['Product'] = df_production_per_product['Product'].str.replace(' ', '') #2021/8/25 追記(重回帰分析計算の説明変数名に空白があるとエラーメッセージが出るため)
        # df_production_per_product_pivot = df_production_per_product.pivot(index=['DateTime','ProductionLine'], columns='Product', values='Number_of_Production').reset_index().copy() #['DateTime','ProductionLine']の列は固定して、行と列を入れ替え
        # #df_production_per_product_pivot.insert(2,"ProductionQuantity",df_production['Number_of_Production'].sum().copy())#1日の生産数量の合計値(全機種対象)を挿入
        # print("===============機種毎の生産数量合計===============")
        # print(df_production_per_product_pivot)

        #生産の順番計算
        # データフレームの列名判定追加 2021/9/8
        if 'Product' in dflist_columns:
            production_order_list = df_production['Product'].tolist().copy() #1日毎の生産機種の順番をリスト化
        elif 'Mold_type' in dflist_columns:
            production_order_list = df_production['Mold_type'].tolist().copy() #1日毎の金型の順番をリスト化
        else:
            print("There is not df_columns...?")
        # production_order_list = df_production['Product'].tolist().copy() #1日毎の生産機種の順番をリスト化
        production_order_list_temp = production_order_list.copy()
        #コメント化 2021/9/8
        # print("===============production_order_list_temp[-1]================")
        # print(production_order_list_temp[-1])
        order_length = len(production_order_list) #1日毎のロット切り替え数をカウント
        if order_length < 12 :
            production_order_list.extend([str("")]*(12-order_length))
        else:
            pass
        #コメント化 2021/9/8
        # print("===============production_order_list==============")
        # print(production_order_list)      

        # データフレームの列名判定追加 2021/9/8
        if 'Product' in dflist_columns:
            production_order_series = pd.Series(production_order_list , index=['Order01_Product','Order02_Product','Order03_Product','Order04_Product','Order05_Product','Order06_Product','Order07_Product','Order08_Product','Order09_Product','Order10_Product','Order11_Product','Order12_Product'])
        elif 'Mold_type' in dflist_columns:
            production_order_series = pd.Series(production_order_list , index=['Order01_Mold_type','Order02_Mold_type','Order03_Mold_type','Order04_Mold_type','Order05_Mold_type','Order06_Mold_type','Order07_Mold_type','Order08_Mold_type','Order09_Mold_type','Order10_Mold_type','Order11_Mold_type','Order12_Mold_type'])
        else:
            print("There is not df_columns...?")        
        # production_order_series = pd.Series(production_order_list , index=['Order01_Product','Order02_Product','Order03_Product','Order04_Product','Order05_Product','Order06_Product','Order07_Product','Order08_Product','Order09_Product','Order10_Product','Order11_Product','Order12_Product'])
        df_production_order = pd.DataFrame(production_order_series).copy()
        df_production_order = df_production_order.T
        df_production_order.insert(0,"DateTime",df_production_per_product_pivot['DateTime'].head())#1列目にDateTimeのColumnを追加
        #ret = df_production.append(df_production_order_series, ignore_index=True)
        #コメント化 2021/9/8
        # print("===============df_production_order==============")
        # print(df_production_order)

        if order_length > max_order_length:# 1日のロット切り替えの最大数をカウント
            max_order_length = order_length
        else:
            pass
        #print("================max_order_length================")
        #print(max_order_length)

        #段取り替え回数計算
        #2要素を1つずつずらして取り出す1/2
        #まず、リスト内要素を1つずらした新規リストを2つ作成する
        n = 1
        production_order_list2 = production_order_list[n:] + production_order_list[:n] # 要素のインデックスをひとつずらす
        production_order_list.pop() # リストの末尾の要素を削除
        production_order_list2.pop() # リストの末尾の要素を削除
        #print("production_order_listとproduction_order_list2")
        #print(production_order_list, production_order_list2) # 1つずれたリストが2つできる
        setup_count = 0
        for i, j in zip(production_order_list, production_order_list2):
            #print("前生産機種")
            #print(i)
            #print("現在生産機種")
            #print(j) 
            if j != str(""):#現在機種が存在しない
                if i != j:
                    setup_count += 1
                else:
                    pass
            else:
                pass
        #前日の最終生産機種と当日の最初の生産機種が異なる場合、当日の段取り替え回数を1追加
        if last_production_order == str(""):#1回目の処理対応
            setup_count += 1
        elif last_production_order != production_order_list[0]:
            setup_count += 1
        else:
            pass
        #print("======setup_count========")
        #print(setup_count)
        #print("======last_production_order========")
        #print(last_production_order)
        #print("======production_order_list[0]========")
        #print(production_order_list[0])
        last_production_order = production_order_list_temp[-1] #前日の最終生産機種の格納

        #生産数量合計値、段取り替え回数、ロット数をdf_production_per_product_pivotのDataFrameに結合
        #データフレームの列名判定追加 2021/9/8
        if 'Product' in dflist_columns:
            df_production_per_product_pivot.insert(2,"ProductionQuantity",df_production['Number_of_Production'].sum().copy())#1日の生産数量の合計値(全機種対象)を挿入
            df_production_per_product_pivot.insert(3,"Number_of_setup_changes",setup_count)#1日の段取り替え回数
            df_production_per_product_pivot.insert(4,"Number_of_lots",order_length)#1日のロット数    
            print("===============機種毎の生産数量合計===============")
            #print(df_temp[['Product','Number_of_Production']])
            print(df_production_per_product_pivot)
        elif 'Mold_type' in dflist_columns:
            # df_production_per_product_pivot.insert(2,"ProductionQuantity",df_production['Number_of_Production'].sum().copy())#1日の生産数量の合計値(全機種対象)を挿入
            df_production_per_product_pivot.insert(2,"Number_of_setup_changes",setup_count)#1日の段取り替え回数
            df_production_per_product_pivot.insert(3,"Number_of_lots",order_length)#1日のロット数    
            # print("===============機種毎の生産数量合計===============")
            # #print(df_temp[['Product','Number_of_Production']])
            # print(df_production_per_product_pivot)
        else:
            print("There is not df_columns...?")
        # df_production_per_product_pivot.insert(2,"ProductionQuantity",df_production['Number_of_Production'].sum().copy())#1日の生産数量の合計値(全機種対象)を挿入
        # df_production_per_product_pivot.insert(3,"Number_of_setup_changes",setup_count)#1日の段取り替え回数
        # df_production_per_product_pivot.insert(4,"Number_of_lots",order_length)#1日のロット数    
        # print("===============機種毎の生産数量合計===============")
        # #print(df_temp[['Product','Number_of_Production']])
        # print(df_production_per_product_pivot)

        #df_production_per_product_pivotの結合
        if df_production_per_product_concat.empty is False:#DataFrameのempty確認
            df_production_per_product_concat = pd.concat([df_production_per_product_concat,df_production_per_product_pivot])
        else:
            df_production_per_product_concat = df_production_per_product_pivot.copy()

        #df_production_orderの結合
        if df_production_order_concat.empty is False:#DataFrameのempty確認
            df_production_order_concat = pd.concat([df_production_order_concat,df_production_order])
        else:
            df_production_order_concat = df_production_order.copy()

    # データフレームの列名判定追加 2021/9/8
    if 'Product' in dflist_columns:
        df_merge = pd.merge(df_production_per_product_concat,df_production_order_concat) #DateTimeをキーにしてmerge
        df_production_per_product_output = df_production_per_product_concat.drop(columns =["ProductionLine","ProductionQuantity","Number_of_setup_changes","Number_of_lots"]).copy()
    elif 'Mold_type' in dflist_columns:
        df_merge = pd.merge(df_production_per_product_concat,df_production_order_concat) #DateTimeをキーにしてmerge
        print("===============df_merge===============")
        print(df_merge)
        # df_production_per_product_output = df_merge.drop(columns =["ProductionLine","Number_of_setup_changes","Number_of_lots"]).copy()
        df_production_per_product_output = df_merge.drop(columns =["ProductionLine","Number_of_lots"]).copy() #上の行から変更(2021/9/27)
        print("===============df_production_per_product_output===============")
        print(df_production_per_product_output)
    else:
        print("There is not df_columns...?")
    # df_merge = pd.merge(df_production_per_product_concat,df_production_order_concat) #DateTimeをキーにしてmerge
    # df_production_per_product_output = df_production_per_product_concat.drop(columns =["ProductionLine","ProductionQuantity","Number_of_setup_changes","Number_of_lots"]).copy()
    #df_merge.sort_index(axis=1, ascending=True, inplace=True)#column名称に沿ってsort
    return df_merge, df_production_per_product_output

# #---------------------------------------------------
# #   機種毎の生産数量/段取り替え回数/ロット数を算出(2021/9/8 再編集)
# #---------------------------------------------------
# def product_type_classify_not_per_day(df_production_number):

#     dflist_columns = df_production_number.columns.values 
#     product_type_list = df_production_number['Mold_type'].unique()
#     print("================product_type_list ===================")
#     print(product_type_list)
#     print("================len(product_type_list)===================")
#     print(len(product_type_list))
    
#     # 格納するDataFrameを作成
#     #column_name_list = []
#     column_name_list = ['DateTime']
#     column_name_list.extend(product_type_list)
#     df_production_agg = pd.DataFrame(columns = column_name_list)
#     #df_production_agg = pd.DataFrame(columns = product_type_list)
#     #df_production_agg.insert(1,columns = product_type_list)
#     #print("==================df_production_agg==================")
#     #print(df_production_agg)

#     #同日の生産機種(金型)を抜き出す
#     #df_production_number['DateTime']= df_production_number['DateTime'].dt.strftime("%Y/%m/%d").copy()
#     print("================df_production_number===================")
#     print(df_production_number)
#     df_production_number['DateTime'] = df_production_number['DateTime'].dt.strftime("%Y/%m/%d").copy()
#     df_production_number['DateTime'] = pd.to_datetime(df_production_number['DateTime'], format="%Y/%m/%d").copy()
#     df_merge =  pd.DataFrame()
#     df_production_per_product_concat = pd.DataFrame()
#     df_production_order_concat = pd.DataFrame()
#     for date, df_production in df_production_number.groupby('DateTime'):

#         #金型毎の生産数量合計計算
#         df_production_per_product = df_production.groupby(['DateTime','Mold_type','ProductionLine']).sum().reset_index().copy()
#         df_production_per_product.drop(columns=['Shot_count'])
#         df_production_per_product['Mold_type'] = str("Shot_count_") + df_production_per_product['Mold_type']
#         df_production_per_product['Mold_type'] = df_production_per_product['Mold_type'].str.replace(' ', '') #2021/8/25 追記(重回帰分析計算の説明変数名に空白があるとエラーメッセージが出るため)
#         df_production_per_product_pivot = df_production_per_product.pivot(index=['DateTime','ProductionLine'], columns='Mold_type', values='Shot_count').reset_index().copy() #['DateTime','ProductionLine']の列は固定して、行と列を入れ替え


    

#         #生産数量合計値、段取り替え回数、ロット数をdf_production_per_product_pivotのDataFrameに結合
#         # df_production_per_product_pivot.insert(2,"ProductionQuantity",df_production['Number_of_Production'].sum().copy())#1日の生産数量の合計値(全機種対象)を挿入
#         df_production_per_product_pivot.insert(2,"Number_of_setup_changes",setup_count)#1日の段取り替え回数
#         df_production_per_product_pivot.insert(3,"Number_of_lots",order_length)#1日のロット数    
#         # print("===============機種毎の生産数量合計===============")
#         # #print(df_temp[['Product','Number_of_Production']])
#         # print(df_production_per_product_pivot)

#         #df_production_per_product_pivotの結合
#         if df_production_per_product_concat.empty is False:#DataFrameのempty確認
#             df_production_per_product_concat = pd.concat([df_production_per_product_concat,df_production_per_product_pivot])
#         else:
#             df_production_per_product_concat = df_production_per_product_pivot.copy()

#         #df_production_orderの結合
#         if df_production_order_concat.empty is False:#DataFrameのempty確認
#             df_production_order_concat = pd.concat([df_production_order_concat,df_production_order])
#         else:
#             df_production_order_concat = df_production_order.copy()

#     # データフレームの列名判定追加 2021/9/8
#     df_merge = pd.merge(df_production_per_product_concat,df_production_order_concat) #DateTimeをキーにしてmerge
#     print("===============df_merge===============")
#     print(df_merge)
#     df_production_per_product_output = df_merge.drop(columns =["ProductionLine","Number_of_setup_changes","Number_of_lots"]).copy()
#     print("===============df_production_per_product_output===============")
#     print(df_production_per_product_output)

#     # df_merge = pd.merge(df_production_per_product_concat,df_production_order_concat) #DateTimeをキーにしてmerge
#     # df_production_per_product_output = df_production_per_product_concat.drop(columns =["ProductionLine","ProductionQuantity","Number_of_setup_changes","Number_of_lots"]).copy()
#     #df_merge.sort_index(axis=1, ascending=True, inplace=True)#column名称に沿ってsort
#     return df_production_per_product_output

#---------------------------------------------------
#   1時間単位の生産数量データのResampling　2021/8/25追加
#---------------------------------------------------
def production_per_hour_calculate(df_production_pre_resample):
    df_production_no_resample = df_production_pre_resample[['ProductionLine','Mold_type']].copy()
    print("===========df_production_no_resample=========")   
    print(df_production_no_resample)
    
    df_production_resample = df_production_pre_resample[['Shot_count']].copy()
    print("===========df_production_resample=========")   	
    print(df_production_resample)
    
    df_production_resampled = df_production_resample.resample('1H').sum().copy()
    df_product_per_hour = pd.concat([df_production_no_resample, df_production_resampled], join='inner',axis = 1)
    return df_product_per_hour

#---------------------------------------------------
#   1時間単位の生産数量データを1日単位にResampling　2021/9/3追加
#---------------------------------------------------
def production_per_day_calculate(df_production_pre_resample):
    # df_production_no_resample = df_production_pre_resample[['ProductionLine']].copy()
    production_line = df_production_pre_resample.iloc[1,0]
    # print("===========df_production_no_resample=========")   
    # print(df_production_no_resample)

    df_production_resample = df_production_pre_resample[['Shot_count']].copy()
    print("===========df_production_resample=========")   	
    print(df_production_resample)

    # df_production_resample_index = df_production_resample.index
    # print(type(df_production_resample_index))
    
    df_production_resampled = df_production_resample.resample('1D').sum().copy()
    # df_production_resampled_index = df_production_resampled.index.astype(str).to_list()
    # print(df_production_resampled_index)
    print("===========df_production_resampled=========")   	
    print(df_production_resampled)

    df_product_per_day = df_production_resampled

    # df_product_per_day = pd.concat([df_production_no_resample, df_production_resampled], join='inner',axis = 1)
    df_product_per_day.insert(0,'ProductionLine', production_line)
    print("===========df_product_per_day=========")   	
    print(df_product_per_day)
    return df_product_per_day

# 直接実行されたとき、メイン関数呼び出し
if __name__ == '__main__':
	main.main()