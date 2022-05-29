#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
#
# Copyright (C) 2021 #
# @Time    : 2022/5/28
# @Author  : Huang Sheng
# @File    : match_data_soda342.py

from netCDF4 import Dataset, num2date, date2num
import datetime
import psycopg2
import numpy as np
import os
from multiprocessing import Pool
import imageio

sodaSourcePath = r'E:/do4/soda342/'
source_db = 'SODA342_2db_v0_i0_grid_intlev'
# target_db = 'GRIDDING_SODA342_2db_v0_i0_grid_intlev_match_all'
resolution = 0.5

depthList = np.array(
    [5.034, 15.101, 25.219, 35.358, 45.576, 55.853, 66.262, 76.803, 87.577, 98.623, 110.096, 122.107, 134.909, 148.747,
     164.054, 181.312, 201.263, 224.777, 253.068, 287.551, 330.008, 382.365, 446.726, 524.982, 618.703, 728.692,
     854.994, 996.715, 1152.376, 1319.997, 1497.562, 1683.057, 1874.788, 2071.252, 2271.323, 2474.043, 2678.757,
     2884.898, 3092.117, 3300.086, 3508.633, 3717.567, 3926.813, 4136.251, 4345.864, 4555.566, 4765.369, 4975.209,
     5185.111, 5395.023])

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


def match_soda_var(items):
    file = items[0]
    target_db = items[1]
    conn = psycopg2.connect(database="do", user="postgres", password="1q!@hyes0913", host="localhost", port="5432")
    cur = conn.cursor()

    nc = Dataset(sodaSourcePath + file)
    for var in nc.variables.keys():
        if var == 'yt_ocean':
            lat_arr = nc.variables[var][:].data
        elif var == 'xt_ocean':
            lon_arr = nc.variables[var][:].data
        elif var == 'time':
            times = nc.variables[var][:].data
            time_unit = nc.variables[var].units
        ## 4d
        elif var == 'salt':
            salt_arr = nc.variables[var][:].data
        elif var == 'temp':
            temp_arr = nc.variables[var][:].data
        elif var == 'prho':
            prho_arr = nc.variables[var][:].data
        elif var == 'u':
            u_arr = nc.variables[var][:].data
        elif var == 'v':
            v_arr = nc.variables[var][:].data
        elif var == 'wt':
            w_arr = nc.variables[var][:].data
        ## 3d
        elif var == 'mlp':
            mlp_arr = nc.variables[var][:].data
        elif var == 'mls':
            mls_arr = nc.variables[var][:].data
        elif var == 'mlt':
            mlt_arr = nc.variables[var][:].data
        elif var == 'net_heating':
            net_heating_arr = nc.variables[var][:].data
        elif var == 'ssh':
            ssh_arr = nc.variables[var][:].data
        elif var == 'taux':
            taux_arr = nc.variables[var][:].data
        elif var == 'tauy':
            tauy_arr = nc.variables[var][:].data

    for time in times:
        real_time = num2date(time, time_unit, "standard")
        year = real_time.year
        month = real_time.month
        searchSQL = "SELECT \"Longitude\"::float,\"Latitude\"::float,\"depth(m)\"::float,\"Oxy_mean\"::FLOAT,\"Oxy_median\"::FLOAT from \"{2}\" WHERE " \
                    "\"Year\" = '{0}' and \"Month\" = '{1}'".format(str(year), str(month), source_db)
        cur.execute(searchSQL)
        rows = cur.fetchall()
        print(month, 'month,', 'o2 total:', len(rows))
        if len(rows) == 0:
            continue

        for row in rows:
            lon = row[0]
            lat = row[1]
            depth = row[2]
            oxy_mean = row[3]
            oxy_median = row[4]
            # 找数据对应的下标
            depth_index = np.where(depthList == depth)[0][0]
            lat_index = int((lat - np.min(lat_arr)) / resolution)
            lon_index = int((lon - np.min(lon_arr)) / resolution)
            month_index = month - 1
            if lat_index >= 330 or lat_index < 0 or lon_index >= 720 or lon_index < 0:
                continue

            salt = salt_arr[month_index, depth_index, lat_index, lon_index]
            temp = temp_arr[month_index, depth_index, lat_index, lon_index]
            prho = prho_arr[month_index, depth_index, lat_index, lon_index]
            u = u_arr[month_index, depth_index, lat_index, lon_index]
            v = v_arr[month_index, depth_index, lat_index, lon_index]
            w = w_arr[month_index, depth_index, lat_index, lon_index]

            mlp = mlp_arr[month_index, lat_index, lon_index]
            mls = mls_arr[month_index, lat_index, lon_index]
            mlt = mlt_arr[month_index, lat_index, lon_index]
            net_heating = net_heating_arr[month_index, lat_index, lon_index]
            ssh = ssh_arr[month_index, lat_index, lon_index]
            taux = taux_arr[month_index, lat_index, lon_index]
            tauy = tauy_arr[month_index, lat_index, lon_index]

            sql = "INSERT INTO \"{0}\"(\"Year\",\"Month\",\"Longitude\",\"Latitude\",\"depth(m)\",\"Oxy_mean\",\"Oxy_median\"," \
                  "\"u\",\"v\",\"w\",\"temp\",\"salt\",\"ssh\",\"taux\",\"tauy\",\"mlp\",\"mls\",\"mlt\",\"net_heating\",\"prho\")" \
                  " VALUES ('{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','{17}','{18}','{19}','{20}')". \
                format(target_db, year, month, lon, lat, depth, oxy_mean, oxy_median, u, v, w, temp, salt, ssh, taux,
                       tauy, mlp, mls, mlt, net_heating, prho)

            # sql = "INSERT INTO \"{9}\"(\"Year\",\"Month\",\"Longitude\",\"Latitude\",\"depth(m)\",\"Oxy_mean\",\"Oxy_median\",\"{0}\")" \
            #       " VALUES ('{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}')" \
            #     .format(valueName, year, month, lon, lat, depth, oxy_mean, oxy_median, value, target_db)
            cur.execute(sql)
            conn.commit()
    nc.close()
    conn.close()

    return


lon_resolution = 0.5
lat_resolution = 0.5
lon_min = 0.25
lon_max = 359.75
lat_min = -74.75
lat_max = 89.75
lat_upper_bnds = int((lat_max - lat_min) / lat_resolution)
lon_upper_bnds = int((lon_max - lon_min) / lon_resolution)
lat_lower_bnds = 0
lon_lower_bnds = 0
lat_dimension = lat_upper_bnds + 1
lon_dimension = lon_upper_bnds + 1
etop_data = imageio.imread(r'F:\DO\ETOP\ETOPO1_Bed_resample_geotiff.tif')
sea_num_nc = Dataset(r'E:/do4/sea_region/ocean_area_0.5deg.nc')
sea_num_data = sea_num_nc.variables['area_num'][:]
etop_arr = np.array(etop_data)
sea_num_arr = np.array(sea_num_data)
target_db = 'GRIDDING_SODA342_2db_v0_i0_grid_intlev_match_all_copy1'

def match_background_var(year):
    etop_lon_min = -179.75
    etop_lon_max = 179.75
    etop_lat_min = -89.75
    etop_lat_max = 89.75
    sea_region_lon_min = 0.25
    sea_region_lon_max = 359.75
    sea_region_lat_min = -89.75
    sea_region_lat_max = 89.75

    ## ocean_area_0.5deg.nc
    for month in range(1, 13):
        conn = psycopg2.connect(database="do", user="postgres", password="1q!@hyes0913", host="localhost", port="5432")
        cur = conn.cursor()

        searchSQL = "SELECT \"Longitude\"::float,\"Latitude\"::float,\"depth(m)\"::float,\"Oxy_mean\"::FLOAT,\"Oxy_median\"::FLOAT from \"{2}\" WHERE " \
                    "\"Year\" = '{0}' and \"Month\" = '{1}'".format(str(year), str(month), source_db)
        cur.execute(searchSQL)
        rows = cur.fetchall()
        print(month, 'month,', 'o2 total:', len(rows))
        if len(rows) == 0:
            continue

        for row in rows:
            lon = row[0]
            lat = row[1]
            depth = row[2]
            oxy_mean = row[3]
            oxy_median = row[4]
            # 找etop数据对应的下标
            if lon >= 180:
                etop_lon_index = int((lon - 360 - etop_lon_min) / lon_resolution)

            elif lon < 180:
                etop_lon_index = int((lon - etop_lon_min) / lon_resolution)

            etop_lat_index = int((lat - etop_lat_min) / lat_resolution)

            etop = etop_arr[etop_lat_index, etop_lon_index]

            sea_num_lat_index = int((lat - sea_region_lat_min) / lat_resolution)
            sea_num_lon_index = int((lon - sea_region_lon_min) / lon_resolution)
            sea_num = sea_num_arr[sea_num_lat_index, sea_num_lon_index]

            lat_index = int((lat - lat_min) / resolution)
            lon_index = int((lon - lon_min) / resolution)
            depth_index = np.where(depthList == depth)[0][0]

            space_id = depth_index * 330 * 720 + lat_index * 720 + lon_index
            time_id = (year - 1980) * 12 + month - 1

            sql = "INSERT INTO \"{0}\"(\"Year\",\"Month\",\"Longitude\",\"Latitude\",\"depth(m)\",\"Oxy_mean\",\"Oxy_median\"," \
                  "\"ETOPO1_Bed\",\"sea_num\",\"station_id\",\"time_id\")" \
                  " VALUES ('{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}')". \
                format(target_db, year, month, lon, lat, depth, oxy_mean, oxy_median,
                       etop, sea_num, space_id, time_id)


            cur.execute(sql)
            conn.commit()

        conn.close()

    return


if __name__ == '__main__':
    conn = psycopg2.connect(database="do", user="postgres", password="1q!@hyes0913", host="localhost",
                            port="5432")
    cur = conn.cursor()
    # target_db = 'GRIDDING_SODA342_2db_v0_i0_grid_intlev_match_all'
    # file_items = []
    # for root, dirs, files in os.walk(sodaSourcePath):
    #     file_items += files
    # items = np.c_[file_items, np.repeat(target_db, len(file_items))]

    # pool = Pool(15)
    # pool.map(match_soda_var, items)
    # pool.close()
    # pool.join()


    items = [i for i in range(1980, 2020)]
    pool = Pool(15)
    pool.map(match_background_var, items)
    pool.close()
    pool.join()
