#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
#
# Copyright (C) 2021 #
# @Time    : 2021/6/30 10:24
# @Author  : Shao Jian
# @Email   : _Shaojian@zju.edu.cn
# @File    : wod_quality_control_flag34.py

# 5个qc + 1个all qc：
#
# 1.在数据库质量控制标志旁边，应用了几个过滤器。去除了最大和最小观察到的氧气之间差异小于 5 μmol kg -1 的轮廓（qc_flag_0），
#     以及氧气差异小于 0.5 μmol kg -1 的轮廓在 18 个深度级别内 （连续18个点）（qc_flag_1）
#         i) 这些方法确实排除了具有恒定值的配置文件以及以错误的单位描述（通常是 ml l -1）存储的配置文件
# 2.在表面具有小于100 μmol kg -1氧的轮廓被去除  （整个水柱去掉）（qc_flag_2）
# 3.额外的质量控制措施是去除深度超过 200 m 的过饱和度100%（qc_flag_3），以及超过 115% 的过饱和度的剖面 （温盐计算公式）（qc_flag_4）

import psycopg2
import numpy as np
import time as tt
import os
import math
from multiprocessing import Pool as ThreadPool
import sys

conn = psycopg2.connect(database="do", user="postgres", password="1q!@hyes0913", host="localhost", port="5432")
cur = conn.cursor()
# db_list = ["OSD_new"]
# db = 'CTD'
# flag = 'f5'
db = sys.argv[1]
flag = sys.argv[2]


#   Enter water temperature (℃).
#   Enter the salinity (ppt).
#   Enter the elevation (feet above sea level).
#   Output dissolved oxygen saturation (mg/l).
def cal_o2sat(temp, sal, depth):
    temp_k = temp + 273.15

    o2sat_temp_sal = math.exp(-139.34411 + 157570.1 / temp_k - 66423080 / math.pow(temp_k, 2) + 12438000000 /
                              math.pow(temp_k, 3) - 862194900000 / math.pow(temp_k, 4) -
                              sal * (0.017674 - 10.754 / temp_k + 2140.7 / math.pow(temp_k, 2)))

    # c1B27 = (math.pow((10), (((2.880814) - (((depth) / (19748.2)))))))
    # c1B29 = (((((0.000975) - (((0.00001426) * (temp))))) + (((0.00000006436) * (math.pow((temp), (2)))))))
    # c1B25 = (math.exp((((((11.8571) - (((3840.7) / (temp_k))))) - (((216961) / (math.pow((temp_k), (2)))))))))
    # c1B28 = (((c1B27) / (760)))
    # c1B30 = (((((((((1) - (((c1B25) / (c1B28))))) * (((1) - (((c1B29) * (c1B28))))))) / (((1) - (c1B25))))) * (
    #     ((1) - (c1B29)))))
    # c1B31 = o2sat_temp_sal if depth == 0 else (o2sat_temp_sal * c1B28) * c1B30
    # o2sat_mg_L = c1B31

    return o2sat_temp_sal


#   Enter dissolved oxygen saturation (mg/l).
#   Output dissolved oxygen saturation (umol/kg).
def transform_unit_o2(o2sat_mg_L):
    o2sat_umol_kg = o2sat_mg_L * 44.4 / 1.4 * 0.9737
    return o2sat_umol_kg


def process(wod_unique):
    qc_flag_3 = True
    qc_flag_4 = True
    qc_flag_1 = True
    qc_flag_0 = True
    qc_flag_2 = True
    qc_flag_5 = True

    data_sql = "select \"ISO_country\", \"Cruise_ID\", \"Latitude\", \"Longitude\", \"Year\", \"Month\", \"Day\", \"Time\", \"WOD_unique\", " \
               "\"depth(m)\", \"Temp\", \"Sal\", \"Oxy\" from \"{0}_new\" where \"WOD_unique\" = '{1}' order by \"WOD_unique\"::float".format(
        db, wod_unique[0])
    cur.execute(data_sql)
    data_all = cur.fetchall()
    # print(data_all)

    # """qc_flag_1：profile在 18 个深度级别内氧气差异小于 0.5 μmol kg -1，则qc_flag_1不通过(False)
    #               即滑动窗口大小为18，若任一滑动窗口中氧气差异小于 0.5 μmol kg -1，则该profile的qc_flag_1不通过(False)。注意：profile数据量小于18时，用最大数据量做
    # """
    # data_all_np = np.array(data_all)
    # depth_len = len(data_all_np[:,12]) # 深度level个数
    # depth_id=np.array(data_all_np[:,9],dtype=float).argsort()
    # data_all_sort=data_all_np[depth_id] # 按深度排序
    # # qc_flag_1：在滑动窗口大小为18，若任一滑动窗口中氧气差异小于 0.5 μmol kg -1，则该profile的qc_flag_1_2不通过(False)
    # O2con=np.array(data_all_sort[:,12],dtype=float)
    # if depth_len<18:
    #     if(max(O2con)-min(O2con)<0.5):
    #         qc_flag_1 = False
    # else:
    #     for i in range(0,depth_len-17):
    #         if max(O2con[i:i + 18])-min(O2con[i:i + 18])<0.5:
    #             qc_flag_1 = False
    #             break

    # qc_flag_0
    # qc_flag_2
    # qc_flag_3: 额外的质量控制措施是去除深度超过 200 m 的过饱和度100%（qc_flag_3）
    # qc_flag_4：以及超过 115% 的过饱和度的剖面 （温盐计算公式）（qc_flag_4）
    # qc_flag_3/4：一条数据有问题，整个profile标注
    o2_arr = []
    for data in data_all:
        o2_arr.append(float(data[12]))

        if float(data[9]) <= 5.0000 and float(data[12]) < 100:
            qc_flag_2 = False

        o2sat_mg_L = cal_o2sat(float(data[10]), float(data[11]), float(data[9]))
        o2sat_umol_kg = transform_unit_o2(o2sat_mg_L)
        if float(data[9]) <= 50:
            if o2sat_umol_kg * 1.15 < float(data[12]):
                qc_flag_5 = False
        elif float(data[9]) <= 200:
            if o2sat_umol_kg * 1.15 < float(data[12]):
                qc_flag_3 = False
        else:
            if o2sat_umol_kg < float(data[12]):
                qc_flag_4 = False

    max_o2 = max(o2_arr)
    min_o2 = min(o2_arr)
    delta_o2 = max_o2 - min_o2
    if delta_o2 < 5:
        qc_flag_0 = False

    """qc_flag_1：profile在 18 个深度级别内氧气差异小于 0.5 μmol kg -1，则qc_flag_1不通过(False)
                          即滑动窗口大小为18，若任一滑动窗口中氧气差异小于 0.5 μmol kg -1，则该profile的qc_flag_1不通过(False)。注意：profile数据量小于18时，用最大数据量做
            """
    data_all_np = np.array(data_all)
    depth_len = len(data_all_np[:, 12])  # 深度level个数
    depth_id = np.array(data_all_np[:, 9], dtype=float).argsort()
    data_all_sort = data_all_np[depth_id]  # 按深度排序
    # qc_flag_1：在滑动窗口大小为18，若任一滑动窗口中氧气差异小于 0.5 μmol kg -1，则该profile的qc_flag_1_2不通过(False)
    O2con = np.array(data_all_sort[:, 12], dtype=float)
    # if depth_len<18:
    #     if(max(O2con)-min(O2con)<0.5):
    #         qc_flag_1 = False
    # else:
    #     for i in range(0,depth_len-17):
    #         if max(O2con[i:i + 18])-min(O2con[i:i + 18])<0.5:
    #             qc_flag_1 = False
    #             break

    if depth_len == 1:
        qc_flag_1 = True
        qc_flag_0 = True
    else:
        # if max > 20, loose the limitation
        if max_o2 > 20:
            if depth_len <= 18:
                # if min(O2dep) < 3000:
                if max(O2con) - min(O2con) < 0.5:
                    qc_flag_1 = False
            else:
                qc_flag_1 = False
                for i in range(0, depth_len - 17):
                    # if min(O2dep[i:i + 18])>3000:
                    #     break
                    if max(O2con[i:i + 18]) - min(O2con[i:i + 18]) >= 0.5:
                        qc_flag_1 = True
                        break
        else:
            if depth_len <= 18:
                # if min(O2dep) < 3000:
                if max(O2con) - min(O2con) < 0.5:
                    qc_flag_1 = False
            else:
                for i in range(0, 2):
                    # if min(O2dep[i:i + 18])>3000:
                    #     break
                    if max(O2con[i:i + 18]) - min(O2con[i:i + 18]) < 0.5:
                        qc_flag_1 = False
                        break

    insert_sql = "insert into \"WOD_unique_quality_control_{7}_{8}\"(\"WOD_unique\",qc_flag_0,qc_flag_1,qc_flag_2,qc_flag_3,qc_flag_4,qc_flag_5) values" \
                 "({0},{1},{2},{3},{4},{5},{6})".format(wod_unique[0], qc_flag_0, qc_flag_1, qc_flag_2, qc_flag_3, qc_flag_4, qc_flag_5, db, flag)

    cur.execute(insert_sql)
    conn.commit()
    # print(wod_unique[0])


if __name__ == '__main__':
    print(db)
    create_table_sql = "drop table if exists \"WOD_unique_quality_control_{0}_{1}\"; " \
                       "create table \"WOD_unique_quality_control_{0}_{1}\" ( like \"WOD_unique_quality_control_CTD\" INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES );" \
        .format(db, flag)
    cur.execute(create_table_sql)
    conn.commit()

    wod_unique_sql = "select distinct \"WOD_unique\" from \"{0}_new\"".format(db)
    cur.execute(wod_unique_sql)
    wod_uniques = cur.fetchall()
    print('wod_uniques: ', len(wod_uniques))
    # 测试时开一个线程，跑代码的时候根据计算机核数设置
    pool = ThreadPool(20)
    pool.map(process, wod_uniques)
    pool.close()
    pool.join()

    create_table_sql = "drop table if exists \"{0}_flag_{1}\"; " \
                       "create table \"{0}_flag_{1}\" ( like \"CTD_flag_new\" INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES );" \
        .format(db, flag)
    cur.execute(create_table_sql)
    conn.commit()

    print("update table")

    #insert_sql = "insert into \"{0}_flag_{1}\" select \"ISO_country\",\"Cruise_ID\",\"Latitude\",\"Longitude\",\"Year\",\"Month\",\"Day\",\"Time\",tmp1.\"WOD_unique\"," \
    #             " \"depth(m)\",\"Temp\",\"Sal\",\"Oxy\",qc_flag_0,qc_flag_1,qc_flag_2,qc_flag_3,qc_flag_4,qc_flag_5 from \"WOD_unique_quality_control_{0}_{1}\" " \
    #             " tmp1, \"{0}_new\" tmp2 where tmp1.\"WOD_unique\" = tmp2.\"WOD_unique\"".format(db, flag)
    insert_sql = "insert into \"{0}_flag_{1}\" select \"ISO_country\",\"Cruise_ID\",\"Latitude\",\"Longitude\",\"Year\",\"Month\",\"Day\",\"Time\",tmp1.\"WOD_unique\"," \
                 " \"depth(m)\",\"Temp\",\"Sal\",\"Oxy\",qc_flag_0,qc_flag_1,qc_flag_2,qc_flag_3,qc_flag_4,qc_flag_5 from " \
                 " \"{0}_new\" tmp2 left join \"WOD_unique_quality_control_{0}_{1}\" tmp1 on tmp1.\"WOD_unique\" = tmp2.\"WOD_unique\"".format(db, flag)
    cur.execute(insert_sql)
    conn.commit()

    create_table_sql = "drop table if exists \"{0}_flag_{1}_out\"; " \
                       "create table \"{0}_flag_{1}_out\" ( like \"CTD_flag_new\" INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES );" \
        .format(db, flag)
    cur.execute(create_table_sql)
    conn.commit()

    print("insert table")
    insert_sql = "insert into \"{0}_flag_{1}_out\" (select * from \"{0}_flag_{1}\" where qc_flag_0=true and qc_flag_1=true  and qc_flag_2=true  and qc_flag_3=true" \
                 " and qc_flag_5=true and qc_flag_4=true )".format(db, flag)
    cur.execute(insert_sql)
    conn.commit()

    count_sql = "select count(*) from \"{0}_flag_{1}_out\"".format(db, flag)
    cur.execute(count_sql)
    result = cur.fetchall()
    print("out date done! total ", result, "data")

    if db == 'CTD':
        del_sql = "delete from \"{0}_flag_{1}_out\" where \"ISO_country\"='GB' and \"Cruise_ID\"='12073'".format(db, flag)
        cur.execute(del_sql)
        conn.commit()

        count_sql = "select count(*) from \"{0}_flag_{1}_out\"".format(db, flag)
        cur.execute(count_sql)
        result = cur.fetchall()
        print("del 2001 ctd date done!", result, "data remain")

        del_sql = "delete from \"{0}_flag_{1}_out\" where \"ISO_country\"='NL' and \"Cruise_ID\"='1376'".format(db, flag)
        cur.execute(del_sql)
        conn.commit()

        count_sql = "select count(*) from \"{0}_flag_{1}_out\"".format(db, flag)
        cur.execute(count_sql)
        result = cur.fetchall()
        print("del 1986 ctd date done!", result, "data remain")

    if db == 'OSD':
        del_sql = "delete from \"{0}_flag_{1}_out\" where \"ISO_country\"='SU' and \"Cruise_ID\"='16793'".format(db, flag)
        cur.execute(del_sql)
        conn.commit()

        count_sql = "select count(*) from \"{0}_flag_{1}_out\"".format(db, flag)
        cur.execute(count_sql)
        result = cur.fetchall()
        print("del 1986 osd date done!", result, "data remain")

    cur.close()
    conn.close()
