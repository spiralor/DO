#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
#
# Copyright (C) 2021 #
# @Time    : 2022/4/19 14:51
# @Author  : Shao Jian
# @Email   : _Shaojian@zju.edu.cn
# @File    : data_quantity_plot.py

import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import psycopg2
import numpy as np
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter
import matplotlib.ticker as mticker

conn = psycopg2.connect(database="do", user="postgres", password="1q!@hyes0913", host="localhost", port="5432")
cur = conn.cursor()
proj = ccrs.PlateCarree()
fig = plt.figure(figsize=(12, 7), constrained_layout=True)
gs0 = fig.add_gridspec(5, 5)


for i in range(10,11):
    ax = fig.add_subplot(gs0[i], projection=proj)
    ax.coastlines()
    bounds = [-180, 180, -90, 90]
    ax.set_extent(bounds, crs=proj)

    startY, endY = 1890 + 5*i, 1895 + 5*i
    print(startY, endY)

    dlon, dlat = 90, 45
    xticks = np.arange(-180, 180.1, dlon)
    yticks = np.arange(-90, 90.1, dlat)
    gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=False,
                      linewidth=1, linestyle=':', color='k', alpha=0.8)
    gl.xlocator = mticker.FixedLocator(xticks)
    gl.ylocator = mticker.FixedLocator(yticks)
    ax.set_xticks(xticks, crs=ccrs.PlateCarree())
    ax.set_yticks(yticks, crs=ccrs.PlateCarree())
    ax.xaxis.set_major_formatter(LongitudeFormatter(zero_direction_label=True))
    ax.yaxis.set_major_formatter(LatitudeFormatter())
    ax.set_title(str(startY) + ' - ' + str(endY-1))  # 设置标题

    selsql = "select distinct (ROUND(\"Longitude\"::float / 2)*2),(ROUND(\"Latitude\"::float / 2)*2) from \"2db_flag_v0_out\" where \"Year\"::float >= {0} and \"Year\"::float < {1}"\
        .format(startY, endY)
    # selsql = "select distinct \"Longitude\",\"Latitude\" from \"2db_flag_v0_out_domin_merge_delnan\" where \"Year\"::float >= {0} and \"Year\"::float < {1}"\
    #     .format(startY, endY)
    cur.execute(selsql)
    rows = cur.fetchall()

    for row in rows:
        lon, lat = float(row[0]), float(row[1])
        ax.plot(*(lon, lat), transform=proj, color='royalblue', marker='o', ms=0.5)


# Save the plot by calling plt.savefig() BEFORE plt.show()
#plt.savefig('fig1_data_quantity.pdf')
plt.savefig('fig1_data_quantity-0.5.png')

plt.show()
