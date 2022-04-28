#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
#
# Copyright (C) 2021 #
# @Time    : 2021/11/5 15:24
# @Author  : Shao Jian
# @Email   : _Shaojian@zju.edu.cn
# @File    : balance_spatial.py

import numpy as np
import psycopg2
import random
from multiprocessing import Pool as ThreadPool
import os
import sys

depthList = np.array(
    [5.01, 15.07, 25.28, 35.76, 46.61, 57.98, 70.02, 82.92, 96.92, 112.32, 129.49, 148.96, 171.4, 197.79,
     229.48, 268.46, 317.65, 381.39, 465.91, 579.31, 729.35, 918.37, 1139.15, 1378.57, 1625.7, 1875.11, 2125.01,
     2375.0, 2625.0, 2875.0, 3125.0, 3375.0, 3625.0, 3875.0, 4125.0, 4375.0, 4625.0, 4875.0, 5125.0, 5375.0])

conn = psycopg2.connect(database="do", user="postgres", password="1q!@hyes0913", host="localhost", port="5432")
cur = conn.cursor()

db = sys.argv[1]

# db = "GRIDDING_2db_f4"
# random_seed = 7
random_seed = 0
source_table_name = "{1}_b_seed{0}"
source_table = ""
target_table_name = "{1}_b0_seed{0}"
target_table = ""
copy_table_structure = "drop table if exists \"{0}\"; " \
                       "create table \"{0}\" ( like \"{1}\" INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES );"


def process(tup):
    random_seed_i = tup[1]
    source_table = source_table_name.format(str(random_seed_i), db)
    for year in range(1893, 2011):
        searchSQL = "SELECT \"Year\",\"Month\",\"Longitude\",\"Latitude\",\"depth(m)\",\"Oxy_mean\",u,v,w,\"temp\",salt,ssh,taux,tauy,\"ETOPO1_Bed\"" \
                    ",\"o2_CMIP6\",\"Oxy_median\", woa13_year, station_id, spatial_num, sea_num from \"{0}\" where spatial_num='{1}' and \"Year\"::float={2}".format(
            source_table, tup[0], year)
        cur.execute(searchSQL)
        rows = cur.fetchall()
        if len(rows) == 0:
            continue

        for j in range(int(random_seed_i), int(random_seed_i) + 1):
            random_seed_j = j
            target_table = target_table_name.format(str(random_seed_i), db)
            random.seed(random_seed_j)
            data_chosen = random.sample(rows, 1)
            insertSQL = "insert into \"{0}\"(\"Year\",\"Month\",\"Longitude\",\"Latitude\",\"depth(m)\",\"Oxy_mean\",u,v,w,\"temp\",salt,ssh,taux,tauy,\"ETOPO1_Bed\"" \
                        ",\"o2_CMIP6\",\"Oxy_median\", woa13_year, station_id, spatial_num, sea_num) " \
                        "values{1}".format(target_table, data_chosen[0])
            cur.execute(insertSQL)
            conn.commit()
        # print(tup[0], year)


if __name__ == '__main__':

    for i in range(1, 3):
        random_seed_i = i
        # print("========================", random_seed_i, "start!!", "========================")

        source_table = source_table_name.format(str(random_seed_i), db)

        for j in range(i, i + 1):
            random_seed_j = j
            target_table = target_table_name.format(str(random_seed_i), db)
            copy_table_structure_sql = copy_table_structure.format(target_table, source_table)
            cur.execute(copy_table_structure_sql)
            conn.commit()

        update_stpatial_num_sql = "update \"{0}\" set spatial_num = " \
                                  "(div(\"Longitude\"::numeric,5.0),div((\"Latitude\"::numeric+90),5.0),\"depth(m)\");" \
            .format(source_table)
        cur.execute(update_stpatial_num_sql)
        conn.commit()

        searchSQL = "SELECT distinct(spatial_num) from \"{0}\"".format(source_table)
        cur.execute(searchSQL)
        rows = cur.fetchall()
        tup = np.c_[rows, np.repeat(random_seed_i, len(rows))]

        pool = ThreadPool(processes=70)
        pool.map(process, tup)
        pool.close()
        pool.join()
        # print("========================", random_seed_i, "end!!", "========================")

