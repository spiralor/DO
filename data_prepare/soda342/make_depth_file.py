"""
@File:      make_depth_file.py
@Author:    HuangSheng
@Time:      2022/1/1
@Description:  建立depth文件
"""
from netCDF4 import Dataset
import numpy as np


def make_grid_nc_file():
    # 读取海洋深度文件
    nc_file_path = r'E:\do4\soda342\soda3.4.2_mn_ocean_reg_1980.nc'
    nc_file = Dataset(nc_file_path, mode='r')
    depth = nc_file.variables['st_ocean'][:]
    lat = nc_file.variables['yt_ocean'][:]
    lon = nc_file.variables['xt_ocean'][:]
    # 生成海洋深度文件
    # nc_depth_path = r'E:\do4\soda342_full_depth_height.nc'
    nc_depth_path = r'E:\do4\soda342_full_depth_height_1000.nc'
    nc_depth = Dataset(nc_depth_path, mode='w')
    nc_depth.createDimension('depth', 28)
    nc_depth.createDimension('lat', 330)
    nc_depth.createDimension('lon', 720)
    nc_depth.createVariable('lat', 'f', ('lat'), fill_value=-9.99E33)
    nc_depth.createVariable('lon', 'f', ('lon'), fill_value=-9.99E33)
    nc_depth.variables['lat'][:] = lat
    nc_depth.variables['lon'][:] = lon
    nc_depth.createVariable('depth_height', 'f', ('depth', 'lat', 'lon'), fill_value=-9.99E33)
    for depth_index in range(28):
        depth_num = depth[depth_index]
        print(depth_index, depth_num)
        nc_depth.variables['depth_height'][depth_index, :, :] = np.repeat(depth_num, 330 * 720).reshape(330, 720)
    nc_file.close()
    nc_depth.close()
    return


def make_nc_file():
    nc_file_path = r'E:\do4\soda342\soda3.4.2_mn_ocean_reg_1980.nc'
    nc_file = Dataset(nc_file_path, mode='r')

    # nc_depth_path = r'E:\do4\soda342_depth_height.nc'
    nc_depth_path = r'E:\do4\soda342_depth_height_1000.nc'
    nc_depth = Dataset(nc_depth_path, mode='w')
    nc_depth.createDimension('depth', 28)
    nc_depth.createDimension('time', 1)
    nc_depth.createDimension('lat', 1)
    nc_depth.createDimension('lon', 1)
    nc_depth.createVariable('depth_height', 'f', ('time', 'depth', 'lat', 'lon'), fill_value=-9.99E33, zlib=True)
    for depth_index in range(28):
        nc_depth.variables['depth_height'][:, depth_index, :, :] = nc_file.variables['st_ocean'][depth_index]
    nc_file.close()
    nc_depth.close()
    return


if __name__ == '__main__':
    make_nc_file()
