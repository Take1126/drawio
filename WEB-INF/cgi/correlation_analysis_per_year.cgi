#!/Users/takeuchihiroki/.pyenv/versions/anaconda3-5.2.0/envs/Analysis/bin/python
# coding=utf-8

import main
df_per_day_mold, df_per_day_merged=main.correlation_analysis_per_day()
df_per_month_mold, df_per_month_merged=main.correlation_analysis_per_month(df_per_day_mold, df_per_day_merged)
main.correlation_analysis_per_year(df_per_day_mold, df_per_day_merged, df_per_month_mold, df_per_month_merged)