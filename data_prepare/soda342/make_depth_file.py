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
    nc_file_path = r'E:\do4\soda342_depth_height_bnds.nc'
    nc_file = Dataset(nc_file_path, mode='r')
    depth = nc_file.variables['st_ocean_bnds'][:]
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
        depth_num = depth[depth_index, 1] - depth[depth_index, 0]
        print(depth_index, depth_num)
        nc_depth.variables['depth_height'][depth_index, :, :] = np.repeat(depth_num, 330 * 720).reshape(330, 720)
    nc_file.close()
    nc_depth.close()
    return

def make_grid_nc_file_Indian():
    # 读取海洋深度文件
    nc_file_path_d = r'E:\do4\soda342_depth_height_bnds.nc'
    nc_file_d = Dataset(nc_file_path_d, mode='r')
    depth = nc_file_d.variables['st_ocean_bnds'][:]
    nc_file_d.close()

    nc_file_path = r'E:\do4\soda342\soda3.4.2_mn_ocean_reg_1980.nc'
    nc_file = Dataset(nc_file_path, mode='r')
    lat_s = nc_file.variables['yt_ocean'][:]
    lon_s = nc_file.variables['xt_ocean'][:]
    lon_min = 21.75
    lon_max = 151.25
    lat_min = -49.75
    lat_max = 26.75
    lon_min_index = np.where(lon_s == lon_min)[0][0]
    lon_max_index = np.where(lon_s == lon_max)[0][0] + 1
    lat_min_index = np.where(lat_s == lat_min)[0][0]
    lat_max_index = np.where(lat_s == lat_max)[0][0] + 1
    lon_dimension = int((lon_max - lon_min)/0.5 + 1)
    lat_dimension = int((lat_max - lat_min)/0.5 + 1)

    lat = nc_file.variables['yt_ocean'][lat_min_index:lat_max_index]
    lon = nc_file.variables['xt_ocean'][lon_min_index:lon_max_index]

    # 生成Indian Ocean海洋深度文件
    nc_depth_path = r'E:\do4\soda342_full_depth_height_Indian.nc'
    # nc_depth_path = r'E:\do4\soda342_full_depth_height_1000.nc'
    # depth_dimension = 28
    depth_dimension = 50
    nc_depth = Dataset(nc_depth_path, mode='w')
    nc_depth.createDimension('depth', depth_dimension)
    nc_depth.createDimension('lat', lat_dimension)
    nc_depth.createDimension('lon', lon_dimension)
    nc_depth.createVariable('lat', 'f', ('lat'), fill_value=-9.99E33)
    nc_depth.createVariable('lon', 'f', ('lon'), fill_value=-9.99E33)
    nc_depth.variables['lat'][:] = lat
    nc_depth.variables['lon'][:] = lon
    nc_depth.createVariable('depth_height', 'f', ('depth', 'lat', 'lon'), fill_value=-9.99E33)
    for depth_index in range(depth_dimension):
        depth_num = depth[depth_index,1] - depth[depth_index,0]
        print(depth_index, depth_num)
        nc_depth.variables['depth_height'][depth_index, :, :] = np.repeat(depth_num, lat_dimension * lon_dimension).reshape(lat_dimension, lon_dimension)
    nc_file.close()
    nc_depth.close()
    return

def make_nc_file():
    nc_file_path = r'E:\do4\soda342_depth_height_bnds.nc'
    nc_file = Dataset(nc_file_path, mode='r')

    nc_depth_path = r'E:\do4\soda342_depth_height.nc'
    depth_dimension = 50
    # nc_depth_path = r'E:\do4\soda342_depth_height_1000.nc'
    nc_depth = Dataset(nc_depth_path, mode='w')
    nc_depth.createDimension('depth', depth_dimension)
    # nc_depth.createDimension('time', 1)
    nc_depth.createDimension('lat', 1)
    nc_depth.createDimension('lon', 1)
    # nc_depth.createVariable('depth', 'f', ('time', 'depth', 'lat', 'lon'), fill_value=-9.99E33, zlib=True)
    nc_depth.createVariable('depth_height', 'f', ('depth', 'lat', 'lon'), fill_value=-9.99E33, zlib=True)
    for depth_index in range(depth_dimension):
        depth_value = nc_file.variables['st_ocean_bnds'][depth_index, 1] - nc_file.variables['st_ocean_bnds'][depth_index, 0]
        nc_depth.variables['depth_height'][depth_index] = depth_value
    nc_file.close()
    nc_depth.close()
    return

if __name__ == '__main__':
    # make_grid_nc_file_Indian()
    make_nc_file()
