"""
@File:      PlotConfidenceBelt.py
@Author:    HuangSheng
@Time:      2022/01/27
@Description:  根据时间序列绘制置信带图像
"""

import matplotlib.pyplot as plt
import numpy as np


def PlotConfidenceBelt():
    N = 21
    x = np.linspace(0, 10, 11)
    y = [3.9, 4.4, 10.8, 10.3, 11.2, 13.1, 14.1, 9.9, 13.9, 15.1, 12.5]

    # 拟合线性曲线，估计其y值及其误差
    # a, b = np.polyfit(x, y, deg=1)
    # y_est = a * x + b
    # y_err代表y值的置信带
    y_err_80 = 1.282 * x.std() * np.sqrt(1 / len(x) + (x - x.mean()) ** 2 / np.sum((x - x.mean()) ** 2))
    y_err_90 = 1.645 * x.std() * np.sqrt(1 / len(x) + (x - x.mean()) ** 2 / np.sum((x - x.mean()) ** 2))
    y_err_95 = 1.96 * x.std() * np.sqrt(1 / len(x) + (x - x.mean()) ** 2 / np.sum((x - x.mean()) ** 2))
    y_err_98 = 2.326 * x.std() * np.sqrt(1 / len(x) + (x - x.mean()) ** 2 / np.sum((x - x.mean()) ** 2))
    fig, ax = plt.subplots()
    # ax.plot(x, y_est, '-')
    # ax.fill_between(x, y - y_err_98, y + y_err_98, alpha=0.3)
    # ax.fill_between(x, y - y_err_95, y + y_err_95, alpha=0.3)
    # ax.fill_between(x, y - y_err_90, y + y_err_90, alpha=.3)
    ax.fill_between(x, y - y_err_98, y + y_err_98, alpha=0.15/2, facecolor='C0')
    ax.fill_between(x, y - y_err_95, y + y_err_95, alpha=0.15, facecolor='C0')
    ax.fill_between(x, y - y_err_90, y + y_err_90, alpha=0.3, facecolor='C0')
    ax.fill_between(x, y - y_err_80, y + y_err_80, alpha=0.6, facecolor='C0')
    ax.plot(x, y)

    plt.show()
    return

if __name__ == '__main__':
    PlotConfidenceBelt()
