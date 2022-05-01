#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
#
# Copyright (C) 2021 #
# @Time    : 2021/11/5 10:50
# @Author  : Shao Jian
# @Email   : _Shaojian@zju.edu.cn
# @File    : balance_year.py

import numpy as np
import psycopg2
import random
from multiprocessing import Pool as ThreadPool
import os
import sys

conn = psycopg2.connect(database="do", user="postgres", password="1q!@hyes0913", host="localhost", port="5432")
cur = conn.cursor()

db = sys.argv[1]
# db = 'GRIDDING_OSD_CTD_f4_distinct_delta'
time_windows = np.arange(1893, 2011, 1)
source_table_name = "GRIDDING_data_year_format"
target_table_name = "{1}_b_seed{0}"
target_table = ""
copy_table_structure = "drop table if exists \"{0}\"; " \
                       "create table \"{0}\" ( like \"{1}\" INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES );"

def process(station_index):
    for time_window in time_windows:
        searchSQL = "SELECT \"Year\",\"Month\",\"Longitude\",\"Latitude\",\"depth(m)\",\"Oxy_mean\",u,v,w,\"temp\",salt,ssh,taux,tauy,\"ETOPO1_Bed\"" \
                    ",\"o2_CMIP6\",\"Oxy_median\", woa13_year, station_id, sea_num from \"{2}\" " \
                    "where station_id={0} and \"Year\"::float={1}".format(station_index[0], time_window, db)
        cur.execute(searchSQL)
        rows = cur.fetchall()
        if len(rows) == 0:
            continue
        for i in range(1, 3):
            random_seed = i
            random.seed(random_seed)
            target_table = target_table_name.format(str(random_seed), db)
            data_chosen = random.sample(rows, 1)
            insertSQL = "insert into \"{0}\"(\"Year\",\"Month\",\"Longitude\",\"Latitude\",\"depth(m)\",\"Oxy_mean\",u,v,w,\"temp\",salt,ssh,taux,tauy,\"ETOPO1_Bed\"" \
                    ",\"o2_CMIP6\",\"Oxy_median\", woa13_year, station_id, sea_num) " \
                        "values{1}".format(target_table, data_chosen[0])
            cur.execute(insertSQL)
            conn.commit()
        # print(station_index, time_window)

if __name__ == '__main__':

    for i in range(1, 3):
        random_seed = i
        target_table = target_table_name.format(str(random_seed), db)
        copy_table_structure_sql = copy_table_structure.format(target_table, source_table_name)
        cur.execute(copy_table_structure_sql)
        conn.commit()

    sql = "select distinct station_id from \"{0}\"".format(db)
    cur.execute(sql)
    items = cur.fetchall()
    # items = np.arange(1, station_number + 1, 1)
    pool = ThreadPool(70)
    pool.map(process, items)
    pool.close()
    pool.join()
