#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
#
# Copyright (C) 2021 #
# @Time    : 2022/1/6 17:05
# @Author  : Shao Jian
# @Email   : _Shaojian@zju.edu.cn
# @File    : supfig8-r(1982).py
from netCDF4 import Dataset, num2date
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.pyplot import *

# o2s 1871-2010
o2s_path = r"E:\do4\do_predict\predict\50_ANALYZE\cdo_et_200_none_2_fit_2db_v0_i0_b0_seed1_after1970_cos_lon\o2P_d_m.nc"
o2s_nc = Dataset(o2s_path)

time_units = o2s_nc.variables['time'].units
time = o2s_nc.variables['time'][:]
real_time = num2date(time, time_units, '360_day')
year = []
for i in range(0, len(real_time)):
    year.append(real_time[i].year + (real_time[i].month - 0.5) / 12)
year = np.array(year)

o2s = o2s_nc['o2P'][:][1332-24:1680]
o2s = o2s
# - np.mean(o2s)

# SST 1850-2021
sst_path = r'F:\DO\hadsst\HadSST.4.0.1.0_monthly_GLOBE.csv'
sst_excel = pd.read_csv(sst_path)
sst = np.array(sst_excel['anomaly'])[1584-24:1932]

print('o2s 分层结果')
print('1871-2010:')
r_sst_list = []
o2s_depth_add = np.zeros(348+24)
r_sst_list_add = []
k_list = []
for index in range(40):
    o2s_depth = (o2s[:, index].reshape(-1) - np.mean(o2s[:, index].reshape(-1))) / np.mean(
        o2s[:, index].reshape(-1)) * 100
    o2s_depth_add += o2s_depth
    r_sst = np.corrcoef(o2s_depth, sst)
    r_sst_list.append(r_sst[0, 1])
    r_sst_add = np.corrcoef(o2s_depth_add, sst)
    r_sst_list_add.append(r_sst_add[0, 1])

    k_list.append(np.polyfit(sst, o2s_depth, 1)[0])

x = np.array([5.01, 15.07, 25.28, 35.76, 46.61, 57.98, 70.02, 82.92, 96.92, 112.32, 129.49, 148.96, 171.4, 197.79,
              229.48, 268.46, 317.65, 381.39, 465.91, 579.31, 729.35, 918.37, 1139.15, 1378.57, 1625.7, 1875.11,
              2125.01,
              2375.0, 2625.0, 2875.0, 3125.0, 3375.0, 3625.0, 3875.0, 4125.0, 4375.0, 4625.0, 4875.0, 5125.0, 5375.0])

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

# x_major_locator = MultipleLocator(2)
ax = plt.gca()
# ax为两条坐标轴的实例
# ax.xaxis.set_major_locator(x_major_locator)
# 把x轴的主刻度设置为1的倍数

plt.plot(x, r_sst_list, '-o', markersize=2)
# plt.plot(x, r_gmst_list, '-o', markersize=2)
plt.legend(['sst'])
plt.title('各层相关性系数r')
plt.show()

ax = plt.gca()
# ax为两条坐标轴的实例
# ax.xaxis.set_major_locator(x_major_locator)
# 把x轴的主刻度设置为1的倍数

plt.plot(x, r_sst_list_add, '-o', markersize=2)
# plt.plot(x, r_gmst_list_add, '-o', markersize=2)
plt.legend(['sst', 'gmst'])
plt.title('各层累加相关性系数r')
plt.show()

fig, ax1 = plt.subplots()
plt.title('1980年以来分深度层相关性系数r和斜率k')

ax2 = ax1.twinx()

bar = ax1.bar(x, np.array(r_sst_list) * (-1), width=30, ec='g', ls='--', lw=0.2)
# plt.plot(x, np.array(r_sst_list_add)*(-1), '-o', markersize=2,color='red')

line = ax2.plot(x, np.array(k_list) * (-1), '-o', markersize=2, color='red')
# plt.legend(['各层相关性系数r'])
ax1.legend('r')
ax2.legend('k', bbox_to_anchor=(1, 0.9))

# plt.legend(([bar, line]), (['r','k']))
plt.show()
print(r_sst_list)

