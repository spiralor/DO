import pandas as pd
import numpy as np
import psycopg2

depthList = [5.034, 15.101, 25.219, 35.358, 45.576, 55.853, 66.262, 76.803, 87.577,
             98.623, 110.096, 122.107, 134.909, 148.747, 164.054, 181.312, 201.263,
             224.777, 253.068, 287.551, 330.008, 382.365, 446.726, 524.982, 618.703,
             728.692, 854.994, 996.715, 1152.376, 1319.997, 1497.562, 1683.057, 1874.788,
             2071.252, 2271.323, 2474.043, 2678.757, 2884.898, 3092.117, 3300.086, 3508.633,
             3717.567, 3926.813, 4136.251, 4345.864, 4555.566, 4765.369, 4975.209, 5185.111,
             5395.023]


def analysis_grid_intlevel_wod_data():
    conn = psycopg2.connect(database="do",
                            user="postgres",
                            password="1q!@hyes0913",
                            host="202.121.180.60",
                            port="5432")

    cur = conn.cursor()
    # 查询每年的数据集
    # for year in range(1980, 2019 + 1):
    #     searchSQL = "select count(*) from \"SODA342_2db_v0_i0_grid_intlev\" where \"Year\"::float = {0}".format(year)
    #     cur.execute(searchSQL)
    #     rows = cur.fetchall()
    #     count = rows[0][0]
    #     print(year, count)

    for depth in depthList:
        searchSQL = "select count(*) from \"SODA342_2db_v0_i0_grid_intlev\" where \"depth(m)\"::float = {0}".format(
            depth)
        cur.execute(searchSQL)
        rows = cur.fetchall()
        count = rows[0][0]
        print(depth, count)
    return


if __name__ == '__main__':
    analysis_grid_intlevel_wod_data()
