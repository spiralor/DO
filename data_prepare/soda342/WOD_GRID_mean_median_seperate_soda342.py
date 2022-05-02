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

select_db = '2db_flag_v0_out'
insert_db = 'SODA342_2db_v0_i0_grid_intlev'
lon_resolution = 0.5
lat_resolution = 0.5
lon_min = 0.25
lon_max = 359.75
lat_min = -74.75
lat_max = 89.75
lat_upper_bnds = (lat_max - lat_min) / lat_resolution
lon_upper_bnds = (lon_max - lon_min) / lon_resolution
lat_lower_bnds = 0
lon_lower_bnds = 0
lat_dimension = lat_upper_bnds + 1
lon_dimension = lon_upper_bnds + 1

depthList = np.array([5.034, 15.101, 25.219, 35.358, 45.576, 55.853, 66.262, 76.803, 87.577, 98.623, 110.096,
                      122.107, 134.909, 148.747, 164.054, 181.312, 201.263, 224.777, 253.068, 287.551, 330.008,
                      382.365, 446.726, 524.982, 618.703, 728.692, 854.994, 996.715, 1152.376, 1319.997, 1497.562,
                      1683.057, 1874.788, 2071.252, 2271.323, 2474.043, 2678.757, 2884.898, 3092.117, 3300.086,
                      3508.633, 3717.567, 3926.813, 4136.251, 4345.864, 4555.566, 4765.369, 4975.209, 5185.111,
                      5395.023])
depth_level = len(depthList)  # depth_level = 50

depthInsideList = np.zeros((depth_level,))
for index in range(depth_level):
    if depthList[index] <= 237.5:
        depthInsideList[index] = 50
    elif depthList[index] > 237.5 and depthList[index] <= 875:
        depthInsideList[index] = 100
    elif depthList[index] > 875 and depthList[index] <= 1975:
        depthInsideList[index] = 200
    elif depthList[index] > 1975:
        depthInsideList[index] = 1000

depthOutsideList = np.zeros((depth_level,))
for index in range(depth_level):
    if depthList[index] <= 487.5:
        depthOutsideList[index] = 200
    elif depthList[index] > 487.5 and depthList[index] <= 1275:
        depthOutsideList[index] = 400
    elif depthList[index] > 1275:
        depthOutsideList[index] = 1000


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
        sql = (
            "select \"Year\"::int, \"Month\"::int, \"Longitude\"::float, \"Latitude\"::float, \"depth(m)\"::float, \"Oxy\"::float "
            "from \"2db_flag_v0_out\" where \"Year\"::int = {0} and \"Month\"::int = {1} order by"
            " \"Year\"::int,\"Month\"::int,\"depth(m)\"::float,\"Longitude\"::float,\"Latitude\"::float"). \
            format(year, month, select_db)
        cur.execute(sql)
        rows = cur.fetchall()

        # print("year=", year, "month=", month, "共", len(rows), "条数据")

        array = np.zeros((lat_dimension, lon_dimension))
        gridList = [""] * lon_dimension * lat_dimension
        for index in range(0, len(rows)):
            row_lat = rows[index][3]
            row_lon = rows[index][2]
            row_lat_index = int((row_lat - lat_min) / lat_resolution)
            row_lon_index = int((row_lon - lon_min) / lon_resolution)
            if row_lat_index < lat_lower_bnds or row_lat_index > lat_upper_bnds or row_lon_index < lon_lower_bnds or row_lon_index > lon_upper_bnds:
                continue
            array[row_lat_index, row_lon_index] += 1
            gridList[row_lon_index + lon_dimension * row_lat_index] += (str(index) + ',')
            rows[index] = (rows[index][0], rows[index][1], row_lon_index, row_lat_index, rows[index][4], rows[index][5])

        for index in range(0, lon_dimension * lat_dimension):
            if array[math.floor(index / lon_dimension), index % lon_dimension] == 0:
                continue
            rowString = gridList[index]
            rowt = rowString.split(',')
            gridRow = []
            for temp in range(0, len(rowt) - 1):
                gridRow.append(rows[int(rowt[temp])])

            distanceMatrix = np.zeros((len(gridRow), 1))
            for depthNumber in range(depth_level):
                # above 1 below 2
                lagrangian_flag1 = False
                # above 2 below 1
                lagrangian_flag2 = False

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

                    gridRow_lon = gridRow[distanceMatrixIndex0[0][0]][2] * lon_resolution + lon_min
                    gridRow_lat = gridRow[distanceMatrixIndex0[0][0]][3] * lat_resolution + lat_min
                    result = [year, month, gridRow_lon, gridRow_lat, depthList[depthNumber], y_mean, y_median]
                    insertSQL = "INSERT INTO \"{7}\" (\"Year\", \"Month\", \"Longitude\", \"Latitude\", \"depth(m)\", \"Oxy_mean\", \"Oxy_median\") " \
                                "values('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}')" \
                        .format(result[0], result[1], result[2], result[3], result[4], result[5], result[6],
                                insert_db)
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
                        if (len(gridRow) - 1 < outsideBelow) or (
                                not judgeDepthShred(gridRow[outsideBelow][4], depthList[depthNumber], 1)):
                            lagrangian_flag1 = True
                            pointSet_mean[2] = pointSet_median[2] = [gridRow[insideBelow][4], gridRow[insideBelow][5]]
                        # if len(np.where((distanceMatrix - gridRow[outsideBelow][4] - depthList[depthNumber])<0.0001)[0]) == 1:
                        elif len(np.where(
                                abs(distanceMatrix - gridRow[outsideBelow][4] + depthList[depthNumber]) < 0.000001)[
                                     0]) == 1:
                            pointSet_mean[2] = pointSet_median[2] = [gridRow[insideBelow][4], gridRow[insideBelow][5]]
                            pointSet_mean[3] = pointSet_median[3] = [gridRow[outsideBelow][4], gridRow[outsideBelow][5]]
                        else:
                            pointSet_mean[2] = pointSet_median[2] = [gridRow[insideBelow][4], gridRow[insideBelow][5]]
                            pointSet_mean[3][0] = pointSet_median[3][0] = gridRow[outsideBelow][4]
                            data = []
                            for pointIndex in \
                                    np.where(abs(
                                        distanceMatrix - gridRow[outsideBelow][4] + depthList[depthNumber]) < 0.000001)[
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
                                np.where(
                                    abs(distanceMatrix - gridRow[insideBelow][4] + depthList[depthNumber]) < 0.000001)[
                                    0]:
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
                        if (len(gridRow) - 1 < outsideBelow) or (
                                not judgeDepthShred(gridRow[outsideBelow][4], depthList[depthNumber], 1)):
                            lagrangian_flag1 = True
                        elif len(np.where(
                                abs(distanceMatrix - gridRow[outsideBelow][4] + depthList[depthNumber]) < 0.000001)[
                                     0]) == 1:
                            pointSet_mean[3] = pointSet_median[3] = [gridRow[outsideBelow][4], gridRow[outsideBelow][5]]
                        else:
                            pointSet_mean[3][0] = pointSet_median[3][0] = gridRow[outsideBelow][4]
                            data = []
                            for pointIndex in \
                                    np.where(abs(
                                        distanceMatrix - gridRow[outsideBelow][4] + depthList[depthNumber]) < 0.000001)[
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
                        if (0 > outsideAbove) or (
                                not judgeDepthShred(gridRow[outsideAbove][4], depthList[depthNumber], 1)):
                            lagrangian_flag2 = True
                            pointSet_mean[1] = pointSet_median[1] = [gridRow[insideAbove][4], gridRow[insideAbove][5]]
                        elif len(np.where(
                                abs(distanceMatrix - gridRow[outsideAbove][4] + depthList[depthNumber]) < 0.000001)[
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
                                    np.where(abs(
                                        distanceMatrix - gridRow[outsideAbove][4] + depthList[depthNumber]) < 0.000001)[
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
                                np.where(
                                    abs(distanceMatrix - gridRow[insideAbove][4] + depthList[depthNumber]) < 0.000001)[
                                    0]:
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
                            np.where(abs(distanceMatrix - gridRow[insideAbove][4] + depthList[depthNumber]) < 0.000001)[
                                0])
                        if (0 > outsideAbove) or (
                                not judgeDepthShred(gridRow[outsideAbove][4], depthList[depthNumber], 1)):
                            lagrangian_flag2 = True
                        elif len(np.where(
                                abs(distanceMatrix - gridRow[outsideAbove][4] + depthList[depthNumber]) < 0.000001)[
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
                                        distanceMatrix - gridRow[outsideAbove][4] + depthList[depthNumber]) < 0.000001)[
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
                # print(pointSet)
                if lagrangian_flag1 == False and lagrangian_flag2 == False:
                    rm = 2
                    rn = 1
                    y1_mean = fparab(depthList[depthNumber], pointSet_mean[0:3, 0], pointSet_mean[0:3, 1])
                    y2_mean = fparab(depthList[depthNumber], pointSet_mean[1:4, 0], pointSet_mean[1:4, 1])
                    yr_mean = fref(depthList[depthNumber], pointSet_mean[0:4, 0], pointSet_mean[0:4, 1], rm)
                    # print("year=",year," month=", month, " index=", index)
                    if y1_mean == yr_mean and y2_mean == yr_mean:
                        y_mean = yr_mean
                    else:
                        d1 = abs(yr_mean - y1_mean)
                        d2 = abs(yr_mean - y2_mean) ** rn
                        r1 = d1 / (d1 + d2)
                        r2 = d2 / (d1 + d2)
                        y_mean = r1 * y2_mean + r2 * y1_mean

                    y1_median = fparab(depthList[depthNumber], pointSet_median[0:3, 0], pointSet_median[0:3, 1])
                    y2_median = fparab(depthList[depthNumber], pointSet_median[1:4, 0], pointSet_median[1:4, 1])
                    yr_median = fref(depthList[depthNumber], pointSet_median[0:4, 0], pointSet_median[0:4, 1], rm)
                    # print("year=",year," month=", month, " index=", index)
                    if y1_median == yr_median and y2_median == yr_median:
                        y_median = yr_median
                    else:
                        d1 = abs(yr_median - y1_median)
                        d2 = abs(yr_median - y2_median) ** rn
                        r1 = d1 / (d1 + d2)
                        r2 = d2 / (d1 + d2)
                        y_median = r1 * y2_median + r2 * y1_median
                elif lagrangian_flag1 == False and lagrangian_flag2 == True:
                    y_median = fparab(depthList[depthNumber], pointSet_median[1:4, 0], pointSet_median[1:4, 1])
                    y_mean = fparab(depthList[depthNumber], pointSet_mean[1:4, 0], pointSet_mean[1:4, 1])
                elif lagrangian_flag1 == True and lagrangian_flag2 == False:
                    y_mean = fparab(depthList[depthNumber], pointSet_mean[0:3, 0], pointSet_mean[0:3, 1])
                    y_median = fparab(depthList[depthNumber], pointSet_median[0:3, 0], pointSet_median[0:3, 1])
                else:
                    # y_mean = flin(depthList[depthNumber], pointSet_mean[1:3, 0], pointSet_mean[1:3, 1])
                    # y_median = flin(depthList[depthNumber], pointSet_median[1:3, 0], pointSet_median[1:3, 1])
                    continue

                if y_mean < min(pointSet_mean[1, 1], pointSet_mean[2, 1]) or y_mean > max(pointSet_mean[1, 1],
                                                                                          pointSet_mean[2, 1]):
                    y_mean = flin(depthList[depthNumber], pointSet_mean[1:3, 0], pointSet_mean[1:3, 1])
                if y_median < min(pointSet_median[1, 1], pointSet_median[2, 1]) or y_median > max(pointSet_median[1, 1],
                                                                                                  pointSet_median[
                                                                                                      2, 1]):
                    y_median = flin(depthList[depthNumber], pointSet_median[1:3, 0], pointSet_median[1:3, 1])

                if math.isnan(y_median):
                    print('this is nan ==============================2')
                    print("year=", year, " month=", month, " index=", index)
                    print(pointSet_median)
                    # print(gridRow[outsideAbove])
                    # print(gridRow[insideAbove])
                    # print(gridRow[insideBelow])
                    # print(gridRow[outsideBelow])
                # 5.提取结果，保存到数据库
                # 保存形式："Year" "Month" "Longitude" "Latitude" "depth(m)" "pointX" "pointY" "Oxy"
                # print(index)
                gridRow_lon = gridRow[distanceMatrixIndex0[0][0]][2] * lon_resolution + lon_min
                gridRow_lat = gridRow[distanceMatrixIndex0[0][0]][3] * lat_resolution + lat_min

                result = [year, month, gridRow_lon, gridRow_lat, depthList[depthNumber], y_mean, y_median]

                if y_median < 0:
                    print('this is negative Oxy ==============================')
                    print("year=", year, " month=", month, " index=", index)
                    print(y_median)
                    print(pointSet_median)
                    # print(gridRow[outsideAbove])
                    # print(gridRow[insideAbove])
                    # print(gridRow[insideBelow])
                    # print(gridRow[outsideBelow])

                insertSQL = "INSERT INTO \"{7}\" (\"Year\", \"Month\", \"Longitude\", \"Latitude\", \"depth(m)\", \"Oxy_mean\", \"Oxy_median\") " \
                            "values('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}')" \
                    .format(result[0], result[1], result[2], result[3], result[4], result[5], result[6], insert_db)
                cur.execute(insertSQL)
                conn.commit()
                # print("y_mean=", y_mean, "y_median", y_median)
                # print(pointSet_mean)
                # print(pointSet_median)

    cur.close()
    conn.close()
    print(year, 'END!!!!')


if __name__ == '__main__':
    conn = psycopg2.connect(database="do", user="postgres", password="1q!@hyes0913", host="202.121.180.60", port="5432")
    cur = conn.cursor()
    # items = range(1980, 2019)
    items = range(1981, 1982)
    pool = ThreadPool(20)
    pool.map(process, items)
    pool.close()
    pool.join()
