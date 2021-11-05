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
import copy #
import scipy as sp
from statsmodels.sandbox.regression.predstd import wls_prediction_std
from statsmodels.graphics.regressionplots import plot_partregress_grid
from sklearn.model_selection import train_test_split #
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
	seasonal_decompose_res = sm.tsa.seasonal_decompose(df_electric_power.set_index('DateTime'), period=21)
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
	x = sm.add_constant(x)

	# データ分割したくなったら利用
	# x_train, x_test, y_train, y_test = train_test_split(x, y, train_size =0.8, random_state=0)
	# x_y_train = x_train.join(y_train)

	# 最小二乗法でモデル化
	model = sm.OLS(y.astype(float), x.astype(float))
	result = model.fit()

	# 重回帰分析結果表示
	print("=======================================初期回帰分析結果========================================")
	print(result.summary())

	# 変数間の相関の可視化
	df_concat = pd.concat([explanatory_var, objective_var], axis = 1)
	corr = df_concat.corr()
	# fig, ax = plt.subplots(figsize=(32,24)) 
	#sns.heatmap(corr, square=True, vmax=1, vmin=-1, center=0, annot = True, fmt="1.2f", annot_kws={'size':5}, linewidths = 0.5, cmap = 'Accent')
	# sns.heatmap(corr, square=True, vmax=1, vmin=-1, center=0, annot = True, fmt="1.2f", annot_kws={'size':7}, linewidths = 0.5, cmap = 'coolwarm', ax = ax)
	# #plt.show()
	# plt.savefig('./OutputData/seaborn_heatmap.png', dpi = 200)

	# 回帰分析結果参照用に記載(調べてもまとまっているサイトがなかった)
	# print("=============回帰係数==============")
	# print(result.params)
	# print("=============二乗誤差==============")
	# print(result.bse)
	# print("=============t値==============")
	# print(result.params/result.bse)
	# print("=============p_value==============")
	# print(result.pvalues)
	# print("=============AIC==============")
	# print(result.aic)
	# print("=============BIC==============")
	# print(result.bic)
	# print("===決定係数(McFadden's pseudo-R-squared)===")
	# print(result.prsquared) 

	print("===========================以後除去プロセス============================")

	#変数増加法(決定係数による評価)
	data = pd.get_dummies(explanatory_var).join(y) #説明変数名と目的変数名を含んだデータフレーム
	response = 'PowerConsumption' #説明変数のラベル
	selected = [] #説明変数を格納するためのリスト
	remaining = pd.get_dummies(explanatory_var).columns.to_list() #目的変数名のリスト(結果的に分析に使用しないもの)
	current_score, best_new_score = 0.0, 0.0 
	while remaining and current_score == best_new_score: 
		scores_with_candidates = [] #検索中の決定係数と説明変数名格納用
		for candidate in remaining:
			formula = "{} ~ {} - 1".format(response, ' + '.join(selected + [candidate]))
			score = smf.ols(formula, data).fit().rsquared_adj
			scores_with_candidates.append((score, candidate))
		scores_with_candidates.sort() #昇順に並び替え
		best_new_score, best_candidate = scores_with_candidates.pop() #一番スコアのいいものを除去
		if current_score <= best_new_score: 
			remaining.remove(best_candidate) #分析するためのリストに移動させるため、除去
			selected.append(best_candidate) #分析リストに加える
			current_score = best_new_score 
		print(formula)
	
	formula = "{} ~ {} - 1".format(response, ' + '.join(selected))
	result = smf.ols(formula, data).fit()

	# 重回帰分析の結果を表示する
	print("=======================================増加法回帰分析結果========================================")
	print(result.summary())

	print("====================================除去製品一覧=====================================")
	print(remaining)
	print("====================================================================================")
	
	#変数減少法(t値による評価)
	current_score, best_new_score = 10.0, 10.0
	while selected and current_score == best_new_score:
		scores_with_candidates = []
		formula = "{} ~ {} - 1".format(response, ' + '.join(selected))
		print(formula)
		score = abs(smf.ols(formula, data).fit().tvalues).min() #絶対値が最小のt値のものを格納
		candidate = abs(smf.ols(formula, data).fit().tvalues).idxmin() #絶対値が最小のt値のものの説明変数を格納
		scores_with_candidates.append((score, candidate))
		scores_with_candidates.sort(reverse=True) #降順に並び替え
		best_new_score, best_candidate = scores_with_candidates.pop()  #一番スコアの悪いものを除去
		if best_new_score <= 2.0:
			remaining.append(best_candidate)
			selected.remove(best_candidate)
			current_score = best_new_score
	formula = "{} ~ {} - 1".format(response, ' + '.join(selected))
	result = smf.ols(formula, data).fit()

	# 重回帰分析の結果を表示する
	print("=======================================減少法回帰分析結果========================================")
	print(result.summary())
	
	#変数減少法(t値による評価→負の数を除去する目的で実施)
	current_score, best_new_score = 10.0, 10.0
	while selected and current_score == best_new_score:
		scores_with_candidates = []
		formula = "{} ~ {} - 1".format(response, ' + '.join(selected))
		print(formula)
		score = smf.ols(formula, data).fit().tvalues.min()
		candidate = smf.ols(formula, data).fit().tvalues.idxmin()
		scores_with_candidates.append((score, candidate))
		scores_with_candidates.sort(reverse=True)
		best_new_score, best_candidate = scores_with_candidates.pop()
		if best_new_score <= 2.0:
			remaining.append(best_candidate)
			selected.remove(best_candidate)
			current_score = best_new_score
	formula = "{} ~ {} - 1".format(response, ' + '.join(selected))
	result = smf.ols(formula, data).fit()

	# 重回帰分析の結果を表示する
	print("=======================================最終回帰分析結果========================================")
	print(result.summary())

	print("====================================除去製品一覧=====================================")
	print(remaining)
	print("====================================================================================")
	
	# 変数間の相関の可視化(最終結果)
	df_concat_end = pd.concat([explanatory_var.drop(remaining, axis=1), objective_var], axis = 1)
	corr_end = df_concat_end.corr()
	fig, ax = plt.subplots(figsize=(32,24)) 
	#sns.heatmap(corr, square=True, vmax=1, vmin=-1, center=0, annot = True, fmt="1.2f", annot_kws={'size':5}, linewidths = 0.5, cmap = 'Accent')
	sns.heatmap(corr_end, square=True, vmax=1, vmin=-1, center=0, annot = True, fmt="1.2f", annot_kws={'size':7}, linewidths = 0.5, cmap = 'coolwarm', ax = ax)
	plt.show()
	plt.savefig('./OutputData/seaborn_heatmap_end.png', dpi = 200)

	#https://akitoshiblogsite.com/linear-multiple-regression-statsmodel/
	#fig = plt.figure(figsize=(12,9),dpi = 50)
	#plot_partregress_grid(result, fig=fig)
	#plt.show()

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