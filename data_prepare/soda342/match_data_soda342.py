 #!/usr/bin/python3.6
# -*- coding: utf-8 -*-
#
# Copyright (C) 2021 #
# @Time    : 2022/4/30
# @Author  : Huang Sheng
# @File    : match_data_soda342.py

from netCDF4 import Dataset, num2date
import psycopg2
import numpy as np
import os
from multiprocessing import Pool

sodaSourcePath = r'F:\DO\soda342/'
target_db = 'SODA342_2db_v0_i0_grid_intlev'
db = 'GRIDDING_2db_v0_i0_grid_intlev_copy1'

depthList = np.array(
    [5.01, 15.07, 25.28, 35.76, 46.61, 57.98, 70.02, 82.92, 96.92, 112.32, 129.49, 148.96, 171.4, 197.79,
     229.48, 268.46, 317.65, 381.39, 465.91, 579.31, 729.35, 918.37, 1139.15, 1378.57, 1625.7, 1875.11, 2125.01,
     2375.0, 2625.0, 2875.0, 3125.0, 3375.0, 3625.0, 3875.0, 4125.0, 4375.0, 4625.0, 4875.0, 5125.0, 5375.0])
match_var = ['mlp']
"""
Coordinate System Note:
xt_ocean is consistent with SODA 2.2.4 version longitude, and yt_ocean has 0.25 degree upward shift 
compared to SODA 2.2.4 version latitude; While (xu_ocean, yu_ocean) mesh grid has 0.25 degree upward
shift compared to (xt_ocean, yt_ocean) mesh grid.
More Details:
>>> xt_ocean: -74.75 ~ 89.75 deg, yt_ocean: 0.25 ~ 359.75 deg, 
>>> xu_ocean = xt_ocean + 0.25 deg, yu_ocean = yt_ocean + 0.25 deg,
>>> lat_soda224: -75.25 ~ 89.25 deg, lon_soda224: 0.25 ~ 359.75 deg.
Thus (xt_ocean, yt_ocean) mesh grid  is selected for matching 2D and 3D variable data.
"""


def match_2d_var(file):
    valueName = match_var[0]
    conn = psycopg2.connect(database="do", user="postgres", password="1q!@hyes0913", host="localhost",
                            port="5432")
    cur = conn.cursor()

    nc = Dataset(sodaSourcePath + file)
    for var in nc.variables.keys():
        if var == 'yt_ocean':
            lats_t = nc.variables[var][:].data
        elif var == 'xt_ocean':
            lons_t = nc.variables[var][:].data
        elif var == 'yu_ocean':
            lats_u = nc.variables[var][:].data
        elif var == 'xu_ocean':
            lons_u = nc.variables[var][:].data
        elif var == 'time':
            times = nc.variables[var][:].data
            time_unit = nc.variables[var].units
        elif var == valueName:
            values = nc.variables[var][:].data
        elif var == 'sw_ocean':
            sw_ocean = nc.variables[var][:].data
        elif var == 'st_ocean':
            st_ocean = nc.variables[var][:].data
    flag = sw_ocean == st_ocean
    soda224_nc = Dataset('F:\DO\soda224\sodarename\ssh/1871_ssh.nc')
    lon_224 = np.array(soda224_nc.variables['lon'][:])
    lat_224 = np.array(soda224_nc.variables['lat'][:])
    # flag_lon = lon_224 == lons_t
    # flag_lat = lat_224 == lats_t

    for time in times:
        real_time = num2date(time, time_unit, "standard")
        year = real_time.year
        month = real_time.month
        searchSQL = "SELECT \"Longitude\"::float,\"Latitude\"::float,\"depth(m)\",\"Oxy_mean\"::FLOAT,\"Oxy_median\"::FLOAT from \"{2}\" WHERE " \
                    "\"Year\" = '{0}' and \"Month\" = '{1}'".format(str(year), str(month), db)
        cur.execute(searchSQL)
        rows = cur.fetchall()
        print(month, 'yue,', valueName, ': total', len(rows))
        if len(rows) == 0:
            continue

        for row in rows:
            lon = row[0]
            lat = row[1]
            depth = row[2]
            oxy_mean = row[3]
            oxy_median = row[4]
            # 找数据对应的下标
            # depth_index = np.where(depthList.astype(np.float32) == depth)[0][0]

            """
            Be careful to use T or U coordinate flag.
            """
            lat_index = int((lat - np.min(lat_224)) / 0.5)
            lon_index = int((lon - np.min(lon_224)) / 0.5)
            if lat_index >= 330 or lat_index < 0 or lon_index >= 720 or lon_index < 0:
                continue

            value = values[month - 1, lat_index, lon_index]
            # sql = "UPDATE \"oxy_1973_83\" SET \"{0}\" = ('{1}')" \
            #       "WHERE \"Year\"::float = {2} AND \"Month\"::float = {3} AND" \
            #       "(ABS(\"Longitude\"::float-({4}))<0.0001) AND (ABS(\"Latitude\"::float-({5}))<0.0001)" \
            #     .format(valueName, value, year, month, lon, lat)
            sql = "INSERT INTO \"{9}\"(\"Year\",\"Month\",\"Longitude\",\"Latitude\",\"depth(m)\",\"Oxy_mean\",\"Oxy_median\",\"{0}\")" \
                  " VALUES ('{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}')" \
                .format(valueName, year, month, lon, lat, depth, oxy_mean, oxy_median, value, target_db)
            cur.execute(sql)
            conn.commit()
    nc.close()
    conn.close()

    return


def match_3d_var():
    return


if __name__ == '__main__':
    conn = psycopg2.connect(database="do", user="postgres", password="1q!@hyes0913", host="localhost",
                            port="5432")
    cur = conn.cursor()
    items = []
    for root, dirs, files in os.walk(sodaSourcePath):
        items += files
    pool = Pool(15)
    pool.map(match_2d_var, items)
    pool.close()
    pool.join()
