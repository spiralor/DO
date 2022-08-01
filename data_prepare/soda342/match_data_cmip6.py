from netCDF4 import Dataset, num2date, date2num
import datetime
import psycopg2
import numpy as np
import os
from multiprocessing import Pool

o2_Path = r'E:/do4/MPI-ESM1-2-HR_grid_intlev/year/'
source_db = 'GRIDDING_SODA342_2db_v0_i0_grid_intlev_match_all_Merge'
target_db = 'GRIDDING_SODA342_2db_v0_i0_grid_intlev_match_all_Merge_copy2'

resolution = 0.5
# CMIP6 shape
lat_min = -89.75
lon_min = 0

depthList = np.array(
    [5.034, 15.101, 25.219, 35.358, 45.576, 55.853, 66.262, 76.803, 87.577, 98.623, 110.096, 122.107, 134.909, 148.747,
     164.054, 181.312, 201.263, 224.777, 253.068, 287.551, 330.008, 382.365, 446.726, 524.982, 618.703, 728.692,
     854.994, 996.715, 1152.376, 1319.997, 1497.562, 1683.057, 1874.788, 2071.252, 2271.323, 2474.043, 2678.757,
     2884.898, 3092.117, 3300.086, 3508.633, 3717.567, 3926.813, 4136.251, 4345.864, 4555.566, 4765.369, 4975.209,
     5185.111, 5395.023])


def match_cmip(items):
    file = items[0]
    target_db = items[1]
    conn = psycopg2.connect(database="do", user="postgres", password="1q!@hyes0913", host="localhost", port="5432")
    cur = conn.cursor()

    nc = Dataset(file)
    cmip_o2_arr = nc.variables['o2'][:].data
    times = nc.variables['time'][:].data
    time_unit = nc.variables['time'].units

    for time in times:
        real_time = num2date(time, time_unit, "proleptic_gregorian")
        year = real_time.year
        month = real_time.month
        searchSQL = "SELECT \"Longitude\"::float,\"Latitude\"::float,\"depth(m)\"::float,\"Oxy_mean\"::FLOAT,\"Oxy_median\"::FLOAT, " \
                    "\"u\"::FLOAT,\"v\"::FLOAT,\"w\"::FLOAT,\"temp\"::FLOAT,\"salt\"::FLOAT,\"ssh\"::FLOAT," \
                    "\"taux\"::FLOAT,\"tauy\"::FLOAT,\"ETOPO1_Bed\"::FLOAT,\"station_id\"::FLOAT,\"sea_num\"::FLOAT, " \
                    "\"mlp\"::FLOAT,\"mls\"::FLOAT,\"mlt\"::FLOAT,\"net_heating\"::FLOAT,\"prho\"::FLOAT,\"time_id\"::INT from \"{2}\" WHERE " \
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
            u = row[5]
            v = row[6]
            w = row[7]
            temp = row[8]
            salt = row[9]
            ssh = row[10]
            taux = row[11]
            tauy = row[12]
            etop = row[13]
            station_id = row[14]
            sea_num = row[15]
            mlp = row[16]
            mls = row[17]
            mlt = row[18]
            net_heating = row[19]
            prho = row[20]
            time_id = row[21]

            # 找数据对应的下标
            depth_index = np.where(depthList == depth)[0][0]
            lat_index = int((lat - lat_min) / resolution)
            lon_index = int((lon - lon_min) / resolution)
            month_index = month - 1

            if lat_index >= 360 or lat_index < 0 or lon_index >= 720 or lon_index < 0:
                continue

            cmip_o2 = cmip_o2_arr[month_index, depth_index, lat_index, lon_index]

            sql = "INSERT INTO \"{0}\"(\"Year\",\"Month\",\"Longitude\",\"Latitude\",\"depth(m)\",\"Oxy_mean\",\"Oxy_median\"," \
                  "\"o2_CMIP6\",\"u\",\"v\",\"w\",\"temp\",\"salt\",\"ssh\",\"taux\",\"tauy\",\"ETOPO1_Bed\"," \
                  "\"station_id\",\"sea_num\",\"mlp\",\"mls\",\"mlt\",\"net_heating\",\"prho\",\"time_id\")" \
                  " VALUES ('{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}'," \
                  "'{16}','{17}','{18}','{19}','{20}','{21}','{22}','{23}','{24}','{25}')". \
                format(target_db, year, month, lon, lat, depth, oxy_mean, oxy_median, cmip_o2, u, v, w, temp, salt, ssh,
                       taux, tauy, etop, station_id, sea_num, mlp, mls, mlt, net_heating, prho, time_id)

            cur.execute(sql)
            conn.commit()
    nc.close()
    conn.close()

    return


if __name__ == '__main__':
    conn = psycopg2.connect(database="do", user="postgres", password="1q!@hyes0913", host="localhost",
                            port="5432")
    cur = conn.cursor()

    file_items = []
    for year in range(1980, 2020):
        file_items.append(o2_Path + "o2_MPI-ESM1-2-HR_{0}_grid_intlev.nc".format(year))

    print(file_items)

    items = np.c_[file_items, np.repeat(target_db, len(file_items))]

    pool = Pool(10)
    pool.map(match_cmip, items)
    pool.close()
    pool.join()
