#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
#
# Copyright (C) 2021 #
# @Time    : 2021/7/6 15:50
# @Author  : Shao Jian
# @Email   : _Shaojian@zju.edu.cn
# @File    : WOD_GRID_mean_median.py

import psycopg2
import numpy as np
import math
from operator import itemgetter
from multiprocessing import Pool as ThreadPool
import os, time
import sys
from scipy.interpolate import Akima1DInterpolator

# db_f = sys.argv[1]
db = 'OSD'
db_f = 'f4'

depthList = np.array(
    [5.01, 15.07, 25.28, 35.76, 46.61, 57.98, 70.02, 82.92, 96.92, 112.32, 129.49, 148.96, 171.4, 197.79,
     229.48, 268.46, 317.65, 381.39, 465.91, 579.31, 729.35, 918.37, 1139.15, 1378.57, 1625.7, 1875.11, 2125.01,
     2375.0, 2625.0, 2875.0, 3125.0, 3375.0, 3625.0, 3875.0, 4125.0, 4375.0, 4625.0, 4875.0, 5125.0, 5375.0])
depthInsideList = np.array([5.01, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50,
                            100, 100, 100, 100,
                            200, 200, 200, 200,
                            300, 300, 300, 300,
                            400, 400, 400, 400,
                            500, 500])
depthOutsideList = np.array(
    [200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200,
     400, 400, 400, 400,
     600, 600, 600,
     800, 800, 800, 800,
     1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000])


def flin(x, a, b):
    # dimension a(3), b(3)
    y = b[0] + (x - a[0]) * (b[1] - b[0]) / (a[1] - a[0])
    return y


def fref(x, a, b, rm):
    # dimension a(4),b(4)
    if a[0] < a[1] < x < a[2] < a[3] and rm > 1:
        y12 = flin(x, a[0:2], b[0:2])
        y23 = flin(x, a[1:3], b[1:3])
        y34 = flin(x, a[2:4], b[2:4])
        if y12 == y23 and y23 == y34:
            return y23
        d123 = (y12 - y23) ** rm
        d234 = (y23 - y34) ** rm
        yr = 0.5 * (y23 + (d234 * y12 + d123 * y34) / (d234 + d123))
        return yr
    else:
        return None


def fparab(x, a, b):
    # dimension a(3),b(3)
    # dimension gamma(3)

    gamma = [0, 0, 0]
    gamma[0] = (x - a[1]) * (x - a[2]) / ((a[0] - a[1]) * (a[0] - a[2]))
    gamma[1] = (x - a[2]) * (x - a[0]) / ((a[1] - a[2]) * (a[1] - a[0]))
    gamma[2] = (x - a[0]) * (x - a[1]) / ((a[2] - a[0]) * (a[2] - a[1]))

    yp = gamma[0] * b[0] + gamma[1] * b[1] + gamma[2] * b[2]
    return yp


def judgeDepthShred(depthO, depthS, IOflag):
    if IOflag == 0:
        if abs(depthO - depthS) <= depthInsideList[np.where(depthList == depthS)[0][0]]:
            return True
        else:
            return False
    else:
        if abs(depthO - depthS) <= depthOutsideList[np.where(depthList == depthS)[0][0]]:
            return True
        else:
            return False

def get_median(data):
    data.sort()
    half = len(data) // 2
    return (data[half] + data[~half]) / 2

def process(year):
    conn = psycopg2.connect(database="do", user="postgres", password="1q!@hyes0913", host="202.121.180.60", port="5432")

    cur = conn.cursor()
    for month in range(1, 13):
        # remove calculate avg in pg
        sql = (
            "SELECT * FROM" \
            "(" \
            "(select \"Year\"::int,\"Month\"::int,\"Longitude\"::float+180 as \"Longitude\",\"Latitude\"::float+90 as \"Latitude\",\"depth(m)\"::float, \"Oxy\"::float " \
            "from \"{3}_flag_third_{2}_out\" " \
            "where \"Year\" = '{0}' and \"Month\" = '{1}' " \
            # "group by \"Year\"::int,\"Month\"::int,\"Longitude\"::float,\"Latitude\"::float,\"depth(m)\"::float " \
            ")" \
            # "UNION" \
            # "(select \"Year\"::int,\"Month\"::int,\"Longitude\"::float+180 as \"Longitude\",\"Latitude\"::float+90 as \"Latitude\",\"depth(m)\"::float, \"Oxy\"::float " \
            # "from \"CTD_flag_third_{2}_out\" " \
            # "where \"Year\" = '{0}' and \"Month\" = '{1}' " \
            # # "group by \"Year\"::int,\"Month\"::int,\"Longitude\"::float,\"Latitude\"::float,\"depth(m)\"::float " \
            # ")" \
            # "UNION" \
            # "(select \"Year\"::int,\"Month\"::int,\"Longitude\"::float+180 as \"Longitude\",\"Latitude\"::float+90 as \"Latitude\",\"depth(m)\"::float, \"Oxy\"::float " \
            # "from \"PFL_flag_out_third\" " \
            # "where \"Year\" = '{0}' and \"Month\" = '{1}' " \
            # # "group by \"Year\"::float,\"Month\"::float,\"Longitude\"::float,\"Latitude\"::float,\"depth(m)\"::float " \
            # ")" \
            ") " \
            "AS result where \"Longitude\"::float >= 0 and \"Longitude\"::float <= 360 and \"Latitude\"::float >= 0 and \"Latitude\"::float <= 180" \
            "order by \"depth(m)\"::float,\"Longitude\"::float,\"Latitude\"::float " \
            ) \
            .format(str(year), str(month), db_f, db)
        cur.execute(sql)
        rows = cur.fetchall()

        print("year=", year, "month=", month, "共", len(rows), "条数据")

        array = np.zeros((360, 720, 2))
        gridList = [""] * 720 * 360
        for index in range(0, len(rows)):
            array[math.floor((rows[index][3] if rows[index][3] != 180 else 179.5) / 0.5), math.floor(
                (rows[index][2] if rows[index][2] != 360 else 0) / 0.5), 0] += 1
            gridList[math.floor((rows[index][2] if rows[index][2] != 360 else 0) / 0.5) + math.floor(
                (rows[index][3] if rows[index][3] != 180 else 179.5) / 0.5) * 720] += (str(index) + ',')
            rows[index] = (
                rows[index][0], rows[index][1], math.floor((rows[index][2] if rows[index][2] != 360 else 0) / 0.5),
                math.floor((rows[index][3] if rows[index][3] != 180 else 179.5) / 0.5),
                rows[index][4], rows[index][5])
        index = 0
        for index in range(0, 720 * 360):
            if array[math.floor(index / 720), index % 720, 0] == 0:
                continue
            rowString = gridList[index]
            rowt = rowString.split(',')
            gridRow = []
            for temp in range(0, len(rowt) - 1):
                gridRow.append(rows[int(rowt[temp])])
            print('gridRow:')
            print(gridRow)
            distanceMatrix = np.zeros((len(gridRow), 1))
            for depthNumber in range(0, 40):
                y_mean = -9999999
                y_median = -9999999
                insideAbove = -1
                insideBelow = -1
                outsideAbove = -1
                outsideBelow = -1
                # pointSet = np.zeros((4, 2))
                pointSet_mean = np.zeros((4, 2))
                pointSet_median = np.zeros((4, 2))
                if len(gridRow) == 0:
                    continue
                for temp in range(0, len(gridRow)):
                    distanceMatrix[temp] = float(float(gridRow[temp][4]) - float(depthList[depthNumber]))
                distanceMatrixIndex0 = np.where(distanceMatrix == 0)
                # the point is exactly on the standard level
                if len(distanceMatrixIndex0[0]) != 0:
                    y_mean = 0
                    y_median = 0
                    # mean and media
                    data = []
                    for p in distanceMatrixIndex0[0]:
                        # y += gridRow[p][5]
                        data.append(gridRow[p][5])
                    # y /= len(distanceMatrixIndex0[0])
                    y_mean = np.mean(data)
                    y_median = np.median(data)

                    result = [year, month, gridRow[distanceMatrixIndex0[0][0]][2] / 2 + 0.25,
                              gridRow[distanceMatrixIndex0[0][0]][3] / 2 + 0.25 - 90, depthList[depthNumber], y_mean, y_median]
                    insertSQL = "INSERT INTO \"GRIDDING_{7}_akima_grid_intlev\" (\"Year\", \"Month\", \"Longitude\", \"Latitude\", \"depth(m)\", \"Oxy_mean\", \"Oxy_median\") " \
                                "values('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}')" \
                        .format(result[0], result[1], result[2], result[3], result[4], result[5], result[6], db + db_f)
                    cur.execute(insertSQL)
                    conn.commit()
                    continue
                distanceMatrixIndex1 = np.where(distanceMatrix > 0)
                distanceMatrixIndex2 = np.where(distanceMatrix < 0)
                if len(distanceMatrixIndex1[0]) == 0 or len(distanceMatrixIndex2[0]) == 0:
                    continue
                if len(np.where(distanceMatrix == min(distanceMatrix[distanceMatrixIndex1]))[0]) != 0:
                    if len(np.where(distanceMatrix == min(distanceMatrix[distanceMatrixIndex1]))[0]) == 1:
                        insideBelow = np.where(distanceMatrix == min(distanceMatrix[distanceMatrixIndex1]))[0][0]
                        if not judgeDepthShred(gridRow[insideBelow][4], depthList[depthNumber], 0):
                            continue
                        outsideBelow = insideBelow + 1
                        if len(gridRow) - 1 < outsideBelow:
                            continue
                        if not judgeDepthShred(gridRow[outsideBelow][4], depthList[depthNumber], 1):
                            continue
                        # if len(np.where((distanceMatrix - gridRow[outsideBelow][4] - depthList[depthNumber])<0.0001)[0]) == 1:
                        if len(np.where(
                                abs(distanceMatrix - gridRow[outsideBelow][4] + depthList[depthNumber]) < 0.0001)[
                                   0]) == 1:
                            pointSet_mean[2] = pointSet_median[2] = [gridRow[insideBelow][4], gridRow[insideBelow][5]]
                            pointSet_mean[3] = pointSet_median[3] = [gridRow[outsideBelow][4], gridRow[outsideBelow][5]]
                        else:
                            pointSet_mean[2] = pointSet_median[2] = [gridRow[insideBelow][4], gridRow[insideBelow][5]]
                            pointSet_mean[3][0] = pointSet_median[3][0] = gridRow[outsideBelow][4]
                            data = []
                            for pointIndex in \
                            np.where(abs(distanceMatrix - gridRow[outsideBelow][4] + depthList[depthNumber]) < 0.0001)[
                                0]:
                                data.append(gridRow[pointIndex][5])
                            #     pointSet[3][1] += gridRow[pointIndex][5]
                            # pointSet[3][1] /= len(
                            #     np.where(
                            #         abs(distanceMatrix - gridRow[outsideBelow][4] + depthList[depthNumber]) < 0.0001)[
                            #         0])
                            y_mean = np.mean(data)
                            y_median = np.median(data)
                            pointSet_mean[3][1] = y_mean
                            pointSet_median[3][1] = y_median
                    else:
                        insideBelow = np.where(distanceMatrix == min(distanceMatrix[distanceMatrixIndex1]))[0][0]
                        if not judgeDepthShred(gridRow[insideBelow][4], depthList[depthNumber], 0):
                            continue
                        pointSet_mean[2][0] = pointSet_median[2][0] = gridRow[insideBelow][4]
                        pointIndex = -1
                        data = []
                        for pointIndex in \
                        np.where(abs(distanceMatrix - gridRow[insideBelow][4] + depthList[depthNumber]) < 0.0001)[0]:
                            data.append(gridRow[pointIndex][5])
                            # pointSet[2][1] += gridRow[pointIndex][5]
                        # pointSet[2][1] /= len(
                        #     np.where(abs(distanceMatrix - gridRow[insideBelow][4] + depthList[depthNumber]) < 0.0001)[
                        #         0])
                        y_mean = np.mean(data)
                        y_median = np.median(data)
                        pointSet_mean[2][1] = y_mean
                        pointSet_median[2][1] = y_median
                        outsideBelow = pointIndex + 1
                        if len(gridRow) - 1 < outsideBelow:
                            continue
                        if not judgeDepthShred(gridRow[outsideBelow][4], depthList[depthNumber], 1):
                            continue
                        if len(np.where(
                                abs(distanceMatrix - gridRow[outsideBelow][4] + depthList[depthNumber]) < 0.0001)[
                                   0]) == 1:
                            pointSet_mean[3] = pointSet_median[3] = [gridRow[outsideBelow][4], gridRow[outsideBelow][5]]
                        else:
                            pointSet_mean[3][0] = pointSet_median[3][0] = gridRow[outsideBelow][4]
                            data = []
                            for pointIndex in \
                            np.where(abs(distanceMatrix - gridRow[outsideBelow][4] + depthList[depthNumber]) < 0.0001)[
                                0]:
                                data.append(gridRow[pointIndex][5])
                                # pointSet[3][1] += gridRow[pointIndex][5]
                            # pointSet[3][1] /= len(
                            #     np.where(
                            #         abs(distanceMatrix - gridRow[outsideBelow][4] + depthList[depthNumber]) < 0.0001)[
                            #         0])
                            y_mean = np.mean(data)
                            y_median = np.median(data)
                            pointSet_mean[3][1] = y_mean
                            pointSet_median[3][1] = y_median
                if len(np.where(distanceMatrix == max(distanceMatrix[distanceMatrixIndex2]))[0]) != 0:
                    if len(np.where(distanceMatrix == max(distanceMatrix[distanceMatrixIndex2]))[0]) == 1:
                        insideAbove = np.where(distanceMatrix == max(distanceMatrix[distanceMatrixIndex2]))[0][0]
                        if not judgeDepthShred(gridRow[insideAbove][4], depthList[depthNumber], 0):
                            continue
                        outsideAbove = insideAbove - 1
                        if 0 > outsideAbove:
                            continue
                        if not judgeDepthShred(gridRow[outsideAbove][4], depthList[depthNumber], 1):
                            continue
                        if len(np.where(
                                abs(distanceMatrix - gridRow[outsideAbove][4] + depthList[depthNumber]) < 0.0001)[
                                   0]) == 1:
                            pointSet_mean[1] = pointSet_median[1] = [gridRow[insideAbove][4], gridRow[insideAbove][5]]
                            pointSet_mean[0] = pointSet_median[0] = [gridRow[outsideAbove][4], gridRow[outsideAbove][5]]
                            if math.isnan(pointSet_mean[0][1]):
                                print("ovo ovo")
                        else:
                            pointSet_mean[1] = pointSet_median[1] = [gridRow[insideAbove][4], gridRow[insideAbove][5]]
                            pointSet_mean[0][0] = pointSet_median[0][0] = gridRow[outsideAbove][4]
                            # print(gridRow[outsideAbove])
                            # print(np.where((distanceMatrix - gridRow[outsideAbove][4] - depthList[depthNumber])<0.0001)[0])
                            # 我也不知道这里为什么一定要 np.where(矩阵=(数值-数值))的形式，查出来无论有没有都是空的下标
                            # 而其他地方用 np.where((矩阵-数值)=(数值))的形式是可以的
                            # 改了这个bug就没有nan的数值了
                            data = []
                            for pointIndex in \
                            np.where(abs(distanceMatrix - gridRow[outsideAbove][4] + depthList[depthNumber]) < 0.0001)[
                                0]:
                                data.append(gridRow[pointIndex][5])
                                # pointSet[0][1] += gridRow[pointIndex][5]
                                # print(gridRow[pointIndex])
                            # pointSet[0][1] /= len(np.where(
                            #     abs(distanceMatrix - gridRow[outsideAbove][4] + depthList[depthNumber]) < 0.0001)[0])
                            y_mean = np.mean(data)
                            y_median = np.median(data)
                            pointSet_mean[0][1] = y_mean
                            pointSet_median[0][1] = y_median

                            if math.isnan(pointSet_mean[0][1]):
                                print("ovo ovr")
                    else:
                        insideAbove = np.where(distanceMatrix == min(distanceMatrix[distanceMatrixIndex2]))[0][0]
                        if not judgeDepthShred(gridRow[insideAbove][4], depthList[depthNumber], 0):
                            continue
                        pointSet_mean[1][0] = pointSet_median[1][0] = gridRow[insideAbove][4]
                        pointIndex = -1
                        data = []
                        for pointIndex in \
                        np.where(abs(distanceMatrix - gridRow[insideAbove][4] + depthList[depthNumber]) < 0.0001)[0]:
                            data.append(gridRow[pointIndex][5])
                        #     pointSet[1][1] += gridRow[pointIndex][5]
                        # pointSet[1][1] /= len(
                        #     np.where(abs(distanceMatrix - gridRow[insideAbove][4] + depthList[depthNumber]) < 0.0001)[
                        #         0])
                        y_mean = np.mean(data)
                        y_median = np.median(data)
                        pointSet_mean[1][1] = y_mean
                        pointSet_median[1][1] = y_median
                        outsideAbove = pointIndex - len(
                            np.where(abs(distanceMatrix - gridRow[insideAbove][4] + depthList[depthNumber]) < 0.0001)[
                                0])
                        if 0 > outsideAbove:
                            continue
                        if not judgeDepthShred(gridRow[outsideAbove][4], depthList[depthNumber], 1):
                            continue
                        if len(np.where(
                                abs(distanceMatrix - gridRow[outsideAbove][4] + depthList[depthNumber]) < 0.0001)[
                                   0]) == 1:
                            # pointSet[1] = [gridRow[insideAbove][4], gridRow[insideAbove][5]]
                            pointSet_mean[0] = pointSet_median[0] = [gridRow[outsideAbove][4], gridRow[outsideAbove][5]]
                            if math.isnan(pointSet_mean[0][1]):
                                print("ovr ovo")
                        else:
                            # pointSet[1] = [gridRow[insideAbove][4], gridRow[insideAbove][5]]
                            pointSet_mean[0][0] = pointSet_median[0][0] = gridRow[outsideAbove][4]
                            data = []
                            for pointIndex in \
                                    np.where(abs(
                                        distanceMatrix - gridRow[outsideAbove][4] + depthList[depthNumber]) < 0.0001)[
                                        0]:
                                data.append(gridRow[pointIndex][5])
                            #     pointSet[0][1] += gridRow[pointIndex][5]
                            # pointSet[0][1] /= len(
                            #     np.where(
                            #         abs(distanceMatrix - gridRow[outsideAbove][4] + depthList[depthNumber]) < 0.0001)[
                            #         0])
                            y_mean = np.mean(data)
                            y_median = np.median(data)
                            pointSet_mean[0][1] = y_mean
                            pointSet_median[0][1] = y_median
                            if math.isnan(pointSet_mean[0][1]):
                                print("ovr ovr")
                # # print(pointSet)
                # rm = 2
                # rn = 1
                # y1_mean = fparab(depthList[depthNumber], pointSet_mean[0:3, 0], pointSet_mean[0:3, 1])
                # y2_mean = fparab(depthList[depthNumber], pointSet_mean[1:4, 0], pointSet_mean[1:4, 1])
                # yr_mean = fref(depthList[depthNumber], pointSet_mean[0:4, 0], pointSet_mean[0:4, 1], rm)
                # # print("year=",year," month=", month, " index=", index)
                # if y1_mean == yr_mean and y2_mean == yr_mean:
                #     y_mean = yr_mean
                # else:
                #     d1 = abs(yr_mean - y1_mean)
                #     d2 = abs(yr_mean - y2_mean) ** rn
                #     r1 = d1 / (d1 + d2)
                #     r2 = d2 / (d1 + d2)
                #     y_mean = r1 * y2_mean + r2 * y1_mean
                #
                # y1_median = fparab(depthList[depthNumber], pointSet_median[0:3, 0], pointSet_median[0:3, 1])
                # y2_median = fparab(depthList[depthNumber], pointSet_median[1:4, 0], pointSet_median[1:4, 1])
                # yr_median = fref(depthList[depthNumber], pointSet_median[0:4, 0], pointSet_median[0:4, 1], rm)
                # # print("year=",year," month=", month, " index=", index)
                # if y1_median == yr_median and y2_median == yr_median:
                #     y_median = yr_median
                # else:
                #     d1 = abs(yr_median - y1_median)
                #     d2 = abs(yr_median - y2_median) ** rn
                #     r1 = d1 / (d1 + d2)
                #     r2 = d2 / (d1 + d2)
                #     y_median = r1 * y2_median + r2 * y1_median

                y_mean = Akima1DInterpolator(pointSet_mean[:, 0], pointSet_mean[:, 1])(depthList[depthNumber])
                y_median = Akima1DInterpolator(pointSet_median[:, 0], pointSet_median[:, 1])(depthList[depthNumber])

                # if math.isnan(y):
                #     print('this is nan ==============================2')
                #     print("year=", year, " month=", month, " index=", index)
                #     print(pointSet)
                #     print(gridRow[outsideAbove])
                #     print(gridRow[insideAbove])
                #     print(gridRow[insideBelow])
                #     print(gridRow[outsideBelow])
                # 5.提取结果，保存到数据库
                # 保存形式："Year" "Month" "Longitude" "Latitude" "depth(m)" "pointX" "pointY" "Oxy"
                # print(index)
                result = [year, month, gridRow[outsideAbove][2] / 2 + 0.25, gridRow[outsideAbove][3] / 2 + 0.25 - 90,
                          depthList[depthNumber], y_mean, y_median]
                #
                # if y < 0:
                #     print('this is negative Oxy ==============================')
                #     print("year=", year, " month=", month, " index=", index)
                #     print(y)
                #     print(pointSet)
                #     print(gridRow[outsideAbove])
                #     print(gridRow[insideAbove])
                #     print(gridRow[insideBelow])
                #     print(gRow[outsideBelow])
                insertSQL = "INSERT INTO \"GRIDDING_{7}_akima_grid_intlev\" (\"Year\", \"Month\", \"Longitude\", \"Latitude\", \"depth(m)\", \"Oxy_mean\", \"Oxy_median\") " \
                            "values('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}')" \
                    .format(result[0], result[1], result[2], result[3], result[4], result[5], result[6], db + db_f)
                cur.execute(insertSQL)
                conn.commit()
                print("y_mean=",y_mean,"y_median",y_median)
                print(pointSet_mean)
                print(pointSet_median)

    donesql = "INSERT INTO \"year_{1}\" (\"yeardone\") VALUES ('{0}')".format(year, db + db_f)
    cur.execute(donesql)
    conn.commit()
    cur.close()
    conn.close()
    print(year, '    END!!!!')


if __name__ == '__main__':
    conn = psycopg2.connect(database="do", user="postgres", password="1q!@hyes0913", host="202.121.180.60", port="5432")
    cur = conn.cursor()
    source_db = 'GRIDDING_2db_f6'
    target_db = 'GRIDDING_{0}_akima_grid_intlev'.format(db + db_f)
    create_table_sql = "drop table if exists \"{0}\"; " \
                       "create table \"{0}\" ( like \"{1}\" INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES );" \
        .format(target_db, source_db)
    cur.execute(create_table_sql)
    conn.commit()

    source_db = 'year'
    target_db = 'year_{0}'.format(db + db_f)
    create_table_sql = "drop table if exists \"{0}\"; " \
                       "create table \"{0}\" ( like \"{1}\" INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES );" \
        .format(target_db, source_db)
    cur.execute(create_table_sql)
    conn.commit()
    cur.close()
    conn.close()

    items = range(1871, 2011)
    # items = range(2009, 2011)
    pool = ThreadPool(20)
    pool.map(process, items)
    pool.close()
    pool.join()
