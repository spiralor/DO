# -*- coding: utf-8 -*-

# 模块导入
from netCDF4 import Dataset
from datetime import datetime, timedelta

import numpy as np
# import netCDF4 as nc
from osgeo import gdal, osr
import glob


def num_to_date(hour):
    """nc文件中time字段为距离1900年1月1日0时0分0秒的小时数。现将其转换为标准日期格式'%Y%m%d_%H%M'。"""
    num_day = hour / 24
    date_start = datetime.strptime('19000101_0000', '%Y%m%d_%H%M')
    delta = timedelta(days=num_day)
    date_now = date_start + delta

    return date_now.strftime('%Y%m%d_%H%M')  # 将time转换为'%Y%m%d_%H%M'格式


def create_tiff(variable_name, variable_arr, dates, tiff_dir, lons, lats):
    # 影像的左上角和右下角坐标
    LonMin, LatMax, LonMax, LatMin = [lons.min(), lats.max(), lons.max(), lats.min()]

    # 分辨率计算
    N_lat = len(lats)
    N_lon = len(lons)
    lon_res = (LonMax - LonMin) / (float(N_lon) - 1)
    lat_res = (LatMax - LatMin) / (float(N_lat) - 1)

    # for i in range(len(variable_arr[:])):
    # 创建.tif文件
    driver = gdal.GetDriverByName('GTiff')

    out_tif_name = tiff_dir + dates[i] + '_' + variable_name + '.tif'
    out_tif = driver.Create(out_tif_name, N_lon, N_lat, 1, gdal.GDT_Float32)

    # 设置影像的显示范围
    # -Lat_Res一定要是-的
    geotransform = (LonMin, lon_res, 0, LatMax, 0, -lat_res)
    out_tif.SetGeoTransform(geotransform)

    # 获取地理坐标系统信息，用于选取需要的地理坐标系统
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)  # 定义输出的坐标系为"WGS 84"，AUTHORITY["EPSG","4326"]
    out_tif.SetProjection(srs.ExportToWkt())  # 给新建图层赋予投影信息

    # 数据写出
    variable_arr = variable_arr[::-1]
    variable_arr = np.c_[variable_arr[:, 360:720], variable_arr[:, 0:360]]
    index = np.where(variable_arr == -9.99E33)
    variable_arr[index] = np.nan
    out_tif.GetRasterBand(1).WriteArray(variable_arr)  # 将数据写入内存，此时没有写入硬盘
    out_tif.FlushCache()  # 将数据写入硬盘
    out_tif = None  # 注意必须关闭tif文件

    print("已成功创建" + out_tif_name)


def NC_to_tiffs(data, tiff_dir):
    nc_data_obj = Dataset(data)
    # lons = nc_data_obj.variables['longitude'][:]
    # lats = nc_data_obj.variables['latitude'][:]
    lons = nc_data_obj.variables['lon'][:]
    index = np.where(lons > 180)
    lons[index] = lons[index] - 360
    lats = nc_data_obj.variables['lat'][:]
    times = nc_data_obj.variables['time'][:]
    dates = []
    for time in times:
        date = num_to_date(time)
        dates.append(date)
    # msl_arr = np.asarray(nc_data_obj.variables['msl'])
    # sst_arr = np.asarray(nc_data_obj.variables['sst'])
    # u10_arr = np.asarray(nc_data_obj.variables['u10'])
    # v10_arr = np.asarray(nc_data_obj.variables['v10'])
    #
    # create_tiff('msl', msl_arr, dates, tiff_dir, lons, lats)
    # create_tiff('sst', sst_arr, dates, tiff_dir, lons, lats)
    # create_tiff('u10', u10_arr, dates, tiff_dir, lons, lats)
    # create_tiff('v10', v10_arr, dates, tiff_dir, lons, lats)

    # o2_arr = np.zeros((1,1,330,720))
    # for i in range(330):
    #     for j in range(720):
    #         if (nc_data_obj.variables['temp'][0,0,i,j] != 'nan'):
    #             o2_arr[0,0,i,j] = nc_data_obj.variables['temp'][0,0,i,j]

    # 0时间0层
    o2_arr = np.array(nc_data_obj.variables['o2_pred_rf'][0, 0, :, :])
    create_tiff('o2_pred_rf', o2_arr, dates, tiff_dir, lons, lats)


if __name__ == '__main__':
    # input_folder = '/home/wx/GAN_Data/mode_data/netcdf_data/'
    # output_folder = '/home/wx/GAN_Data/mode_data/tiff_data_0.75/'
    # o2_regrid_1900-01_to_1900-12
    # o2_Omon_MPI-ESM1-2-HR_historical_r1i1p1f1_gn_190001-190412
    input_path = 'C:/Users/26982/Desktop/DO Experiment/cdo_et_400_None_2' \
                 'db_f4_del1986_seed1_balance_cos_lon/1950_do_predict_zip.nc'
    output_folder = 'C:/Users/26982/Desktop/DO Experiment/cdo_et_400_None_2' \
                    'db_f4_del1986_seed1_balance_cos_lon/'

    # 读取所有nc数据
    data_list = glob.glob(input_path)

    for i in range(len(data_list)):
        data = data_list[i]
        NC_to_tiffs(data, output_folder)
        print('----%s转换结束----' % data)
