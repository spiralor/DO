"""
@File:      OxyStorageTimeSeries.py
@Author:    HuangSheng
@Time:      2021/12/24
@Description:  给定固定的经纬度坐标，计算该点的时间序列, 画密度散点图
"""

from netCDF4 import num2date, Dataset
import numpy as np
import matplotlib.pyplot as plt
import os
import pandas as pd
from scipy.interpolate import interpn


def TimeSeries(root_path, auto_keys=None):
    """
    Parameters: root_path.
    Returns: time series of global ocean dissolved o2 storage.
    """

    o2s_nc_path = root_path + r'/o2P_Indian.nc'
    # o2s_nc_path = root_path + r'/o2P_0_1000.nc'
    # o2s_nc_path = root_path + r'/o2P_1000_5000.nc'
    print(o2s_nc_path)
    o2s_nc = Dataset(o2s_nc_path)
    o2 = o2s_nc.variables['o2P'][:]
    time = o2s_nc.variables['time'][:]
    time_units = o2s_nc.variables['time'].units
    x = [num2date(time, time_units, calendar='360_day')[index].year for index in range(140)]
    y = o2.reshape(140, )
    # y = y - np.mean(y)
    # 查找对应年份
    index = np.where(np.array(x) == 1960)
    # print(index[0])
    index = np.where(np.array(x) == 2010)
    # print(index[0])
    plt_title = str(root_path.split('/')[-1])
    return x, y, plt_title


def density_scatter(x, y, sort=True, bins=20, **kwargs):
    """
    Scatter plot colored by 2d histogram
    """
    data, x_e, y_e = np.histogram2d(x, y, bins=bins)
    z = interpn((x_e[:-1], y_e[:-1]), data, np.vstack([x, y]).T, method='splinef2d', bounds_error=False)

    # Sort the points by density, so that the densest points are plotted last
    if sort:
        idx = z.argsort()
        x, y, z = x[idx], y[idx], z[idx]
    # img = ax.scatter(x, y, c=z, **kwargs)
    # return img
    return x, y, z


if __name__ == '__main__':

    root_path = 'C:/Users/26982/Desktop/DO Experiment/Indian/cdo_et_200_None_fit_2dbnew_o2sat_seed1_after1970_cos_lon_Indian'
    x1, y1, title1 = TimeSeries(root_path=root_path)

    root_path = r'C:/Users/26982/Desktop/DO Experiment/Indian/cdo_et_200_20_2_fit_2dbf5_t_seed1_after1970_Indian_cos_lon'
    x2, y2, title2 = TimeSeries(root_path=root_path)

    # root_path = r'C:/Users/26982/Desktop/DO Experiment/Arctic/cdo_et_200_none_2_fit_OSDnew_NEWshred2_seed1_after1970_Arctic_cos_lon'
    # x3, y3, title3 = TimeSeries(root_path=root_path)
    #
    # root_path = 'C:/Users/26982/Desktop/DO Experiment/Arctic/cdo_et_200_none_2_fit_CTDnew_NEWshred2_seed1_after1970_Arctic_cos_lon'
    # x4, y4, title4 = TimeSeries(root_path=root_path)
    #
    # root_path ='C:/Users/26982/Desktop/DO Experiment/cdo_et_400_20_fit_2db_f4_del1986_seed1_after1970_coslon_lt20_sw100'
    # x5, y5, title5 = TimeSeries(root_path=root_path)
    #
    # root_path = 'C:/Users/26982/Desktop/DO Experiment/cdo_et_400_20_fit_2db_f4_del1986_ice_delwoa3_seed1_after1970_coslon_lt20_sw100'
    # x6, y6, title6 = TimeSeries(root_path=root_path)

    # root_path = 'C:/Users/26982/Desktop/DO Experiment/cdo_et_400_20_fit_2db_f4_del1986_seed1_after1970_del_lon'
    # x7, y7, title7 = TimeSeries(root_path=root_path)
    #
    # root_path = 'Z:/do_predict/predict/ExtraTreesRegressor/ANALYZE/cdo_et_fit_2db_seed17_100_in_best_params/'
    # x8, y8, title8 = TimeSeries(root_path=root_path)
    #
    # root_path = 'Z:/do_predict/predict/ExtraTreesRegressor/ANALYZE/cdo_et_fit_2db_seed18_100_in_best_params/'
    # x9, y9, title9 = TimeSeries(root_path=root_path)
    #
    # root_path = 'Z:/do_predict/predict/ExtraTreesRegressor/ANALYZE/cdo_et_fit_2db_seed19_100_in_best_params/'
    # x10, y10, title10 = TimeSeries(root_path=root_path)

    """
        Description: 算上层0-1000m的下降率 回归
        Reference: Ito et al., 2017
    """
    params_list = []
    # params1 = np.polyfit(x1[87:], y1[87:], deg=1)
    # params2 = np.polyfit(x2[87:], y2[87:], deg=1)
    # params3 = np.polyfit(x3[87:], y3[87:], deg=1)
    # params4 = np.polyfit(x4[87:], y4[87:], deg=1)
    # params5 = np.polyfit(x5[87:], y5[87:], deg=1)
    # params6 = np.polyfit(x6[87:], y6[87:], deg=1)
    # params7 = np.polyfit(x7[87:], y7[87:], deg=1)
    # params8 = np.polyfit(x8[87:], y8[87:], deg=1)
    # params9 = np.polyfit(x9[87:], y9[87:], deg=1)
    # params10 = np.polyfit(x10[87:], y10[87:], deg=1)
    # params_list.append(params1[0] * 10000)
    # params_list.append(params2[0] * 10000)
    # params_list.append(params3[0] * 10000)
    # params_list.append(params4[0] * 10000)
    # params_list.append(params5[0] * 10000)
    # params_list.append(params6[0] * 10000)
    # params_list.append(params7[0] * 10000)
    # params_list.append(params8[0] * 10000)
    # params_list.append(params9[0] * 10000)
    # params_list.append(params10[0] * 10000)

    # params_list.append((y1[139] - y1[89]) / (2010-1958) * 10000)
    # params_list.append((y2[139] - y2[89]) / (2010-1958) * 10000)
    # params_list.append((y3[139] - y3[89]) / (2010-1958) * 10000)
    # params_list.append((y4[139] - y4[89]) / (2010-1958) * 10000)
    # params_list.append((y5[139] - y5[89]) / (2010-1958) * 10000)
    # params_list.append((y6[139] - y6[89]) / (2010-1958) * 10000)
    # params_list.append((y7[139] - y7[89]) / (2010-1958) * 10000)
    # params_list.append((y8[139] - y8[89]) / (2010-1958) * 10000)
    # params_list.append((y9[139] - y9[89]) / (2010-1958) * 10000)
    # params_list.append((y10[139] - y10[89]) / (2010-1958) * 10000)
    # for index in range(6):
    #     print('cdo_et_fit_2db_seed{0}_in_best_params: '.format(10 + index), str(params_list[index]), 'Tmol/decade')
    #
    # print('Mean: ', np.mean(params_list), 'Tmol/decade')
    # print('Stddev: ', np.std(params_list), 'Tmol/decade')
    # print('end')

    """
        Description: 1960-2010年全深度总下降率/% 回归
        Reference: Sunke Schmidtko et al., 2017
    """
    # params_list = []
    params1 = np.polyfit(x1[89:], y1[89:], deg=1)
    params2 = np.polyfit(x2[89:], y2[89:], deg=1)
    # params3 = np.polyfit(x3[89:], y3[89:], deg=1)
    # params4 = np.polyfit(x4[89:], y4[89:], deg=1)
    # params5 = np.polyfit(x5[89:], y5[89:], deg=1)
    # params6 = np.polyfit(x6[89:], y6[89:], deg=1)
    # params7 = np.polyfit(x7[89:], y7[89:], deg=1)
    # params8 = np.polyfit(x8[89:], y8[89:], deg=1)
    # params9 = np.polyfit(x9[89:], y9[89:], deg=1)
    # params10 = np.polyfit(x10[89:], y10[89:], deg=1)
    # 计算下降率
    # params_list.append(params1[0] * 50 / y1[89] * 100)
    # params_list.append(params2[0] * 50 / y2[89] * 100)
    # params_list.append(params3[0] * 50 / y3[89] * 100)
    # params_list.append(params4[0] * 50 / y4[89] * 100)
    # params_list.append(params5[0] * 50 / y5[89] * 100)
    # params_list.append(params6[0] * 50 / y6[89] * 100)
    # params_list.append(params7[0] * 50 / y7[89] * 100)
    # params_list.append(params8[0] * 50 / y8[89] * 100)
    # params_list.append(params9[0] * 50 / y9[89] * 100)
    # params_list.append(params10[0] * 50 / y10[89] * 100)
    # 计算下降量
    # params_list.append(params1[0] * 10 * 1000)
    # params_list.append(params2[0] * 50)
    # params_list.append(params3[0] * 50)
    # params_list.append(params4[0] * 50)
    # params_list.append(params5[0] * 50)
    # params_list.append(params6[0] * 50)
    # params_list.append(params7[0] * 50)
    # params_list.append(params8[0] * 50)
    # params_list.append(params9[0] * 50)
    # params_list.append(params10[0] * 50)

    # params_list.append((y1[139] - y1[89]) * 1000 / 5)
    # params_list.append((y2[139] - y2[89]) * 1000 / 5)
    # params_list.append((y3[139] - y3[89]) * 1000 / 5)
    # params_list.append(y4[139] - y4[89])
    # params_list.append(y5[139] - y5[89])
    # params_list.append(y6[139] - y6[89])
    #
    params_list2 = []
    # params_list2.append((y1[139] - y1[89]) / y1[89] * 100)
    # params_list2.append((y2[139] - y2[89]) / y2[89] * 100)
    # params_list2.append((y3[139] - y3[89]) / y3[89] * 100)
    # params_list2.append((y4[139] - y4[89]) / y4[89] * 100)
    # params_list2.append((y5[139] - y5[89]) / y5[89] * 100)
    # params_list2.append((y6[139] - y6[89]) / y6[89] * 100)
    # params_list2.append((y7[139] - y7[89]) / y7[89] * 100)
    # params_list2.append((y8[139] - y8[89]) / y8[89] * 100)
    # params_list2.append((y9[139] - y9[89]) / y9[89] * 100)
    # params_list2.append((y10[139] - y10[89]) / y10[89] * 100)
    title_list = []
    title_list.append(title1)
    title_list.append(title2)
    # title_list.append(title3)
    for index in range(len(params_list2)):
        print(title_list[index], '下降量:', round(params_list[index], 2), 'Tmol/decade,', '下降率:', round(params_list2[index], 2), '%')
    #     print('cdo_et_fit_2db_seed{0}_in_best_params: '.format(index + 1), str(params_list[index]), 'Pmol', str(params_list2[index]), '%')
    #
    # print('Mean: ', np.mean(params_list),'Pmol',np.mean(params_list2),'%')
    # print('Stddev: ', np.std(params_list),'Pmol',np.std(params_list2),'%')
    # print('end')

    # DO-Storage-Time-Series Plot
    # tile = 'o2storage comparison'
    # tile = 'o2storage comparison in upper 1000m'
    # tile = 'o2storage comparison in lower 1000m'
    tile = 'Indian o2storage comparison'
    plt.figure(figsize=(15, 10))
    plt.title(tile)
    plt.xlabel('Year')
    plt.ylabel('o2s/Pmol')
    plt.plot(x1, y1, label=title1)
    plt.plot(x2, y2, label=title2)
    # plt.plot(x3, y3, label=title3)
    # plt.plot(x4, y4, label=title4)
    # plt.plot(x5, y5, label=title5)
    # plt.plot(x6, y6, label=title6)
    # plt.plot(x7, y7, label=title7)
    # plt.plot(x8, y8, label=title8)
    # plt.plot(x9, y9, label=title9)
    # plt.plot(x10, y10, label=title10)
    plt.legend(loc='best')
    plt.show()
