# -*- coding: utf-8 -*-

import pandas as pd
import re
import sys
import os
import io
import openpyxl
import collections
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm
import seaborn as sns
import statsmodels.formula.api as smf
from statsmodels.sandbox.regression.predstd import wls_prediction_std
from statsmodels.graphics.regressionplots import plot_partregress_grid
import main


#---------------------------------------------------
#   生産数量と電力量のDataFrameの結合とデータ整形(分割)
#---------------------------------------------------
def analytical_preparation(electric_power_analysis_data,production_quantity_analysis_data):
	df_production = pd.DataFrame()
	df_electric_power = pd.DataFrame()
	df_production_list = []
	df_electric_power_list = []
	df_production = production_quantity_analysis_data.fillna(0).copy()
	df_electric_power = electric_power_analysis_data.fillna(0).copy()
	df_merge = pd.merge(df_production,df_electric_power,on='DateTime')
	
	# 電力量可視化
	plt.plot(df_electric_power.set_index('DateTime'))
	# 自己相関のグラフ
	fig = plt.figure(figsize=(12,8))
	ax1 = fig.add_subplot(211)
	fig = sm.graphics.tsa.plot_acf(df_electric_power.set_index('DateTime'), lags=150, ax=ax1)
	ax2 = fig.add_subplot(212)
	fig = sm.graphics.tsa.plot_pacf(df_electric_power.set_index('DateTime'), lags=150, ax=ax2)

	# データをトレンドと季節成分に分解
	seasonal_decompose_res = sm.tsa.seasonal_decompose(df_electric_power.set_index('DateTime'), freq=21)
	seasonal_decompose_res.plot()
	plt.show()

	df_production_list = list(df_production.columns)
	df_electric_power_list = list(df_electric_power.columns)

	df_production = df_merge.drop(columns = df_electric_power_list).copy()
	df_electric_power = df_merge.drop(columns = df_production_list).copy()

	print("=============df_production==============")
	print(df_production)
	print("=============df_electric_power==============")
	print(df_electric_power)
    
	return df_production, df_electric_power

#---------------------------------------------------
#   重回帰分析
#---------------------------------------------------
def multiple_regression_analysis(explanatory_var,objective_var):
	x = pd.get_dummies(explanatory_var)# 説明変数
	y = objective_var # 目的変数
		
	# 定数項(y切片)を必要とする線形回帰のモデル式ならば必須
	#X = sm.add_constant(x)

	# 最小二乗法でモデル化
	model = sm.OLS(y.astype(float), x.astype(float))
	result = model.fit()

	# 重回帰分析の結果を表示する
	print(result.summary())

	#https://akitoshiblogsite.com/linear-multiple-regression-statsmodel/
	#fig = plt.figure(figsize=(12,9),dpi = 50)
	#plot_partregress_grid(result, fig=fig)
	#plt.show()


	# 変数間の相関の可視化
	df_concat = pd.concat([explanatory_var, objective_var], axis = 1)
	corr = df_concat.corr()
	fig, ax = plt.subplots(figsize=(32,24)) 
	#sns.heatmap(corr, square=True, vmax=1, vmin=-1, center=0, annot = True, fmt="1.2f", annot_kws={'size':5}, linewidths = 0.5, cmap = 'Accent')
	sns.heatmap(corr, square=True, vmax=1, vmin=-1, center=0, annot = True, fmt="1.2f", annot_kws={'size':7}, linewidths = 0.5, cmap = 'coolwarm', ax = ax)
	#plt.show()
	plt.savefig('./OutputData/seaborn_heatmap.png', dpi = 200)

	return result.summary(), x, y, result

#---------------------------------------------------
#   重回帰分析結果の可視化
#---------------------------------------------------
def multiple_regression_analysis_visual(explanatory_var,objective_var, result):
	fig, ax = plt.subplots(figsize=(16,12))
	ax.plot(explanatory_var, objective_var, 'o', label="data")
	ax.plot(explanatory_var, result.fittedvalues, 'r--.', label="OLS")
	ax.legend(loc='best')
	#plt.show()

	plt.hist(objective_var)
	#plt.show()


#---------------------------------------------------
#   直接実行されたとき、メイン関数呼び出し
#---------------------------------------------------
if __name__ == '__main__':
	main.main()