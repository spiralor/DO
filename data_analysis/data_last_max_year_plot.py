#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
#
# Copyright (C) 2021 #
# @Time    : 2022/4/19 14:51
# @Author  : Shao Jian
# @Email   : _Shaojian@zju.edu.cn

import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import psycopg2
import numpy as np
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter
import matplotlib.ticker as mticker
import matplotlib as mpl

conn = psycopg2.connect(database="do", user="postgres", password="1q!@hyes0913", host="10.21.66.239", port="5432")
cur = conn.cursor()
proj = ccrs.PlateCarree()
fig = plt.figure(figsize=(12, 7), constrained_layout=True)
gs0 = fig.add_gridspec(5, 5)
depthList = [0, 200, 300, 500, 750, 1000, 2000, 2500, 3000, 4000, 5000]

for i in range(5):
    for j in range(2):
        # if i < 4:
        #     continue
        ax = fig.add_subplot(gs0[i, j], projection=proj)
        ax.coastlines()
        bounds = [-180, 180, -90, 90]
        ax.set_extent(bounds, crs=proj)

        startD, endD = depthList[i*2+j], depthList[i*2+j+1]
        print(startD, endD)

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
        ax.set_title(str(endD) + 'm')  # 设置标题
        cmap = plt.get_cmap('cool')


        norm = plt.Normalize(vmin=1890, vmax=2010)

        selsql = "select round(\"Longitude\"::float),round(\"Latitude\"::float),max(\"Year\"::float) from \"2db_flag_v0_out\" where \"depth(m)\"::float >= {0} and \"depth(m)\"::float < {1}" \
                 " group by round(\"Longitude\"::float),round(\"Latitude\"::float)" \
            .format(startD, endD)
        # selsql = "select distinct \"Longitude\",\"Latitude\" from \"2db_flag_v0_out_domin_merge_delnan\" where \"Year\"::float >= {0} and \"Year\"::float < {1}"\
        #     .format(startY, endY)
        cur.execute(selsql)
        rows = cur.fetchall()
        for row in rows:
            lon, lat, maxY = float(row[0]), float(row[1]), (row[2]-1890)//5*5+1890
            # ax.plot(*(lon, lat), transform=proj, marker='o', ms=1, c=maxY)
            ax.scatter(*(lon, lat), transform=proj, marker='o', s=1, c=maxY, cmap=cmap, norm=norm)

        # maxYList = np.zeros(len(rows))
        # for i in range(len(rows)):
        #     maxYList[i] = rows[i][2]

        fig.colorbar(mpl.cm.ScalarMappable(norm=norm, cmap=cmap))
        # im_25 = ax[0][0].imshow(k_data_25, cmap="YlGn")
        # # 增加右侧的颜色刻度条
        # # plt.colorbar(im_25)
        # fig.colorbar(im_25, ax=ax[0][0])

        # ax.plot(*(0, 0), transform=proj, color='royalblue', marker='o', ms=1)


for i in range(5):
    for j in np.arange(3, 5):
        # if i <= 3:
        #     continue
        ax = fig.add_subplot(gs0[i, j], projection=proj)
        ax.coastlines()
        bounds = [-180, 180, -90, 90]
        ax.set_extent(bounds, crs=proj)

        startD, endD = depthList[i*2+j-3], depthList[i*2+j-2]
        print(startD, endD)

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
        ax.set_title(str(endD) + 'm')  # 设置标题
        cmap = plt.get_cmap('cool')

        norm = plt.Normalize(vmin=0, vmax=26)

        selsql = "select round(\"Longitude\"::float),round(\"Latitude\"::float),max(\"Year\"::float)-min(\"Year\"::float) from \"2db_flag_v0_out\" where \"depth(m)\"::float >= {0} and \"depth(m)\"::float < {1}" \
                 " group by round(\"Longitude\"::float),round(\"Latitude\"::float)" \
            .format(startD, endD)
        # selsql = "select distinct \"Longitude\",\"Latitude\" from \"2db_flag_v0_out_domin_merge_delnan\" where \"Year\"::float >= {0} and \"Year\"::float < {1}"\
        #     .format(startY, endY)
        cur.execute(selsql)
        rows = cur.fetchall()
        for row in rows:
            lon, lat, maxY = float(row[0]), float(row[1]), row[2]
            # ax.plot(*(lon, lat), transform=proj, marker='o', ms=1, c=maxY)
            ax.scatter(*(lon, lat), transform=proj, marker='o', s=0.05, c=maxY, cmap=cmap, norm=norm)

        # maxYList = np.zeros(len(rows))
        # for i in range(len(rows)):
        #     maxYList[i] = rows[i][2]

        fig.colorbar(mpl.cm.ScalarMappable(norm=norm, cmap=cmap))
        # im_25 = ax[0][0].imshow(k_data_25, cmap="YlGn")
        # # 增加右侧的颜色刻度条
        # # plt.colorbar(im_25)
        # fig.colorbar(im_25, ax=ax[0][0])

        # ax.plot(*(0, 0), transform=proj, color='royalblue', marker='o', ms=1)

# Save the plot by calling plt.savefig() BEFORE plt.show()
# plt.savefig('fig1_max_year.pdf')
# plt.savefig('fig2_max_last_year.png')

plt.show()
cur.close()
conn.close()
