from netCDF4 import Dataset,num2date
import psycopg2
import numpy as np
import os
import math
from multiprocessing import Pool as ThreadPool
import sys

# sodaSourcePath = 'Y:/soda224/60/ssh/'
sodaSourcePath = r'H:\soda224\sodarename\ssh/'
# sodaSourcePath = 'C:/Users/Administrator/Documents/regrid/60/'
depthList = np.array(
    [5.01, 15.07, 25.28, 35.76, 46.61, 57.98, 70.02, 82.92, 96.92, 112.32, 129.49, 148.96, 171.4, 197.79,
     229.48, 268.46, 317.65, 381.39, 465.91, 579.31, 729.35, 918.37, 1139.15, 1378.57, 1625.7, 1875.11, 2125.01,
     2375.0, 2625.0, 2875.0, 3125.0, 3375.0, 3625.0, 3875.0, 4125.0, 4375.0, 4625.0, 4875.0, 5125.0, 5375.0])
db = sys.argv[1]
source_db = 'ssh1'
target_db = 'ssh'
create_table_sql = "drop table if exists \"{0}\"; " \
                       "create table \"{0}\" ( like \"{1}\" INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES );"\
                        .format(target_db, source_db)

def process(file):
    conn = psycopg2.connect(database="do", user="postgres", password="1q!@hyes0913", host="localhost",
                            port="5432")
    cur = conn.cursor()

    # 三维
    # dir == 'ssh' or dir == 'tauy' or dir == 'taux':
    nc = Dataset(sodaSourcePath + file)
    # print(nc.variables.keys())
    for var in nc.variables.keys():
        if var == 'lat':
            lats = nc.variables[var][:].data
        elif var == 'lon':
            lons = nc.variables[var][:].data
        elif var == 'time':
            times = nc.variables[var][:].data
            time_unit = nc.variables[var].units

        else:
            valueName = var
            values = nc.variables[var][:].data
    # 找时间对应关系
    for time in times:
        real_time = num2date(time, time_unit, "360_day")
        year = real_time.year
        month = real_time.month
        # if month != 12:
        #     continue
        searchSQL = "SELECT \"Longitude\"::float,\"Latitude\"::float,\"depth(m)\",\"Oxy_mean\"::FLOAT,\"Oxy_median\"::FLOAT from \"{2}\" WHERE " \
                    "\"Year\" = '{0}' and \"Month\" = '{1}'".format(str(year), str(month), db)
        cur.execute(searchSQL)
        rows = cur.fetchall()
        print(month, 'Yue,', valueName, ': total', len(rows))
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
            lat_index = int(lat * 2 + 150.5)
            lon_index = int(lon * 2 - 0.5)
            if lat_index >= 330 or lat_index < 0 or lon_index >= 720 or lon_index < 0:
                continue
            # value = values[month - 1, int(lat * 2 + 150.5), int(lon * 2 - 0.5)]
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

if __name__ == '__main__':
    conn = psycopg2.connect(database="do", user="postgres", password="1q!@hyes0913", host="localhost",
                            port="5432")
    cur = conn.cursor()
    cur.execute(create_table_sql)
    conn.commit()
    conn.close()
    items = []
    for root, dirs, files in os.walk(sodaSourcePath):
        items += files
    pool = ThreadPool(15)
    pool.map(process, items)
    pool.close()
    pool.join()
