"""
@File:      ComparisionAndPlotResult.py
@Author:    HuangSheng
@Time:      2022/1/6
@Description:  画密度散点图
"""
from netCDF4 import num2date, Dataset
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
import pandas as pd
from scipy.interpolate import interpn

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


    # 散点图
    # all_data = pd.read_csv('Z:/do_train/extraTree/validation/validation_seed10_before1970.csv', engine='python')
    # x = all_data['y_real']
    # y = all_data['y_pred']


    real_data = pd.read_csv('Z:/do_train/extraTree/validation/validation_seed10_cmip_before1970.csv', engine='python')
    x = real_data['y_cmip'] * 1000 / 1.027
    all_data = pd.read_csv('Z:/do_train/extraTree/validation/validation_seed10_before1970.csv', engine='python')
    y = all_data['y_real']
    title = 'Compare cmip-data with real-data on before1970 dataset'
    density_scatter_x, density_scatter_y, density_scatter_z =\
        density_scatter(x, y)
    plt.title(title)
    plt.xlabel('y_cmip')
    plt.ylabel('y_real')
    plt.scatter(density_scatter_x, density_scatter_y, c=density_scatter_z, cmap='Spectral_r')

    plt.plot(x, x, color='r')
    parameter = np.polyfit(x, y, deg=1)
    p = np.poly1d(parameter)
    plt.plot(x, p(x), color='g')
    plt.colorbar()
    # plt.legend(loc='best')
    plt.show()



