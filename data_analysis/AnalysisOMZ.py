import numpy
import numpy as np
from netCDF4 import Dataset
from multiprocessing import Pool

nc_path = '../predict/2010_do_predict_zip.nc'
nc_file = Dataset(nc_path)
o2 = nc_file.variables['o2_pred_rf'][:]
depth = nc_file.variables['depth'][:]
time = nc_file.variables['time'][:]
lat = nc_file.variables['lat'][:]
lon = nc_file.variables['lon'][:]
time_units = nc_file.variables['time'].units


def cal_o2_min_depth(tup):
    """
    根据索引，求每个水柱单元中最小值对应的深度
    """
    time_index = tup[0]
    lat_index = tup[1]
    lon_index = tup[2]
    o2_list = o2[time_index, :, lat_index, lon_index]
    o2_list = np.array(o2_list)
    o2_list_delete_nan = o2_list[o2_list > 0]
    if o2_list_delete_nan.shape == ((0,)):
        return time_index, lat_index, lon_index, np.nan
    else:
        min = np.min(o2_list_delete_nan)
        o2_min_depth_index = np.where(o2_list == min)
        o2_min_depth = depth[o2_min_depth_index]
        o2_min_depth = np.array(o2_min_depth)[0]
        return time_index, lat_index, lon_index, o2_min_depth


def cal_o2_min_depth_parallel(target_path):
    """
    并行计算330x720网格上的o2最小值对应的深度，保存到目标nc文件
    """
    depth_nc = Dataset(target_path, 'w')
    depth_nc.createDimension('time', 12)
    depth_nc.createDimension('lat', 330)
    depth_nc.createDimension('lon', 720)
    depth_nc.createVariable('time', 'd', ('time'), zlib=True)
    depth_nc.createVariable('lat', 'd', ('lat'), zlib=True)
    depth_nc.createVariable('lon', 'd', ('lon'), zlib=True)
    depth_nc.createVariable('o2_min_depth', 'f', ('time', 'lat', 'lon'), fill_value=-9.99E33, zlib=True)
    depth_nc.variables['time'][:] = time
    depth_nc.variables['time'].units = time_units
    depth_nc.variables['lat'][:] = lat
    depth_nc.variables['lon'][:] = lon
    
    time_index = 6 - 1
    row = 330
    col = 720
    time_list = np.repeat(time_index, row * col)
    lat_index = np.arange(row)
    lon_index = np.arange(col)
    lon_list, lat_list = np.meshgrid(lon_index, lat_index)
    lat_list = lat_list.ravel()
    lon_list = lon_list.ravel()
    print(lat_list)
    print(lon_list)
    tup = np.c_[time_list, lat_list, lon_list]
    print(tup.shape)
    pool = Pool(30)
    res = pool.map(cal_o2_min_depth, tup)
    pool.close()
    pool.join()

    res = numpy.array(res)
    time_index_list, lat_index_list, lon_index_list, o2_min_depth_list = \
        res[:, 0], res[:, 1], res[:, 2], res[:, 3]
    total = len(o2_min_depth_list)
    for index in range(total):
        time_index = time_index_list[index]
        lat_index = lat_index_list[index]
        lon_index = lon_index_list[index]
        o2_min_depth = o2_min_depth_list[index]
        depth_nc.variables['o2_min_depth'][time_index, lat_index, lon_index] = o2_min_depth
        print(index)

    depth_nc.close()
    return

def cal_omz20_upper_depth(tup):

    time_index = tup[0]
    lat_index = tup[1]
    lon_index = tup[2]
    o2_list = o2[time_index, :, lat_index, lon_index]
    o2_list = np.array(o2_list)
    # o2_list_delete_nan = o2_list[o2_list > 0]
    depth_index = np.where((o2_list > 0) & (o2_list <= 20))[0]
    if depth_index.shape == ((0,)):
        return time_index, lat_index, lon_index, np.nan
    else:
        o2_min_depth_index = np.min(depth_index)
        o2_min_depth = depth[o2_min_depth_index]
        # o2_min_depth = np.array(o2_min_depth)[0]
        return time_index, lat_index, lon_index, o2_min_depth


def cal_omz20_upper_depth_parrallel():
    depth_nc = Dataset('omz20_upper_depth.nc', 'w')
    depth_nc.createDimension('time', 12)
    depth_nc.createDimension('lat', 330)
    depth_nc.createDimension('lon', 720)
    depth_nc.createVariable('time', 'd', ('time'), zlib=True)
    depth_nc.createVariable('lat', 'd', ('lat'), zlib=True)
    depth_nc.createVariable('lon', 'd', ('lon'), zlib=True)
    depth_nc.createVariable('o2_min_depth', 'f', ('time', 'lat', 'lon'), fill_value=-9.99E33, zlib=True)
    depth_nc.variables['time'][:] = time
    depth_nc.variables['time'].units = time_units
    depth_nc.variables['lat'][:] = lat
    depth_nc.variables['lon'][:] = lon

    time_index = 6 - 1
    row = 330
    col = 720
    time_list = np.repeat(time_index, row * col)
    lat_index = np.arange(row)
    lon_index = np.arange(col)
    lon_list, lat_list = np.meshgrid(lon_index, lat_index)
    lat_list = lat_list.ravel()
    lon_list = lon_list.ravel()
    print(lat_list)
    print(lon_list)
    # cal_omz20_upper_depth([0,180,500])
    tup = np.c_[time_list, lat_list, lon_list]
    print(tup.shape)
    pool = Pool(3)
    res = pool.map(cal_omz20_upper_depth, tup)
    pool.close()
    pool.join()

    res = numpy.array(res)
    time_index_list, lat_index_list, lon_index_list, o2_min_depth_list = \
        res[:, 0], res[:, 1], res[:, 2], res[:, 3]
    total = len(o2_min_depth_list)
    for index in range(total):
        time_index = time_index_list[index]
        lat_index = lat_index_list[index]
        lon_index = lon_index_list[index]
        o2_min_depth = o2_min_depth_list[index]
        depth_nc.variables['o2_min_depth'][time_index, lat_index, lon_index] = o2_min_depth
        print(index)

    depth_nc.close()

if __name__ == '__main__':
    
    #path = '2010_o2_min_depth.nc'
    #cal_o2_min_depth_parallel(path)
    cal_omz20_upper_depth_parrallel()
