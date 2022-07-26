from netCDF4 import Dataset, num2date
import numpy as np
from multiprocessing import Pool
import numpy.ma as ma
import time as tm

lon_min = 21.75
lon_max = 151.25
lat_min = -49.75
lat_max = 26.75

lon_min_cmip = lon_min - 0.25
lon_max_cmip = lon_max - 0.25
lat_min_cmip = lat_min
lat_max_cmip = lat_max

resolution = 0.5
lon_dim = int((lon_max - lon_min)/resolution) + 1
lat_dim = int((lat_max - lat_min)/resolution) + 1

lon_Indian = np.arange(start=21.75, stop=151.25 + 0.5, step=0.5)
lat_Indian = np.arange(start=-49.75, stop=26.75 + 0.5, step=0.5)

def process(year):

    src_path = "/home/zju/do4/MPI-ESM1-2-HR_grid_intlev/year/o2_MPI-ESM1-2-HR_{0}_grid_intlev.nc".format(str(year))
    src_nc = Dataset(src_path)
    
    lat = src_nc.variables['lat'][:]
    lon = src_nc.variables['lon'][:]
    depth = src_nc.variables['lev'][:]
    depth_bnds = src_nc.variables['lev_bnds'][:]
    time = src_nc.variables['time'][:]
    time_bnds = src_nc.variables['time_bnds'][:]
    time_units = src_nc.variables['time'].units
    cmip_lon_min_index = np.where(lon == lon_min_cmip)[0][0]  
    cmip_lon_max_index = np.where(lon == lon_max_cmip)[0][0] + 1
    cmip_lat_min_index = np.where(lat == lat_min_cmip)[0][0]
    cmip_lat_max_index = np.where(lat == lat_max_cmip)[0][0] + 1
    o2 = src_nc.variables['o2'][:,:,cmip_lat_min_index:cmip_lat_max_index,cmip_lon_min_index:cmip_lon_max_index]
    
    tar_path = "/home/zju/do4/MPI-ESM1-2-HR_grid_intlev/year_Indian/o2_MPI-ESM1-2-HR_{0}_grid_intlev_Indian.nc".format(str(year))
    tar_nc = Dataset(tar_path, 'w')
    tar_nc.createDimension('time', time.shape[0])
    tar_nc.createDimension('depth', depth.shape[0])
    tar_nc.createDimension('lat', lat_dim)
    tar_nc.createDimension('lon', lon_dim)
    tar_nc.createDimension('bnds', 2)

    tar_nc.createVariable('o2', 'f', ('time', 'depth', 'lat', 'lon'), fill_value=-9.99E33, zlib=True)
    tar_nc.createVariable('depth_bnds', 'f', ('depth', 'bnds'), fill_value=-9.99E33, zlib=True)
    tar_nc.createVariable('time_bnds', 'f', ('time', 'bnds'), fill_value=-9.99E33, zlib=True)
    tar_nc.createVariable('time', 'd', ('time'), zlib=True)
    tar_nc.createVariable('depth', 'd', ('depth'), zlib=True)
    tar_nc.createVariable('lat', 'd', ('lat'), zlib=True)
    tar_nc.createVariable('lon', 'd', ('lon'), zlib=True)
    tar_nc.variables['time'][:] = time
    tar_nc.variables['depth'][:] = depth
    tar_nc.variables['depth'].bounds = "depth_bnds"
    tar_nc.variables['time'].bounds = "time_bnds"
    tar_nc.variables['lat'][:] = lat_Indian
    tar_nc.variables['lon'][:] = lon_Indian
    tar_nc.variables['time_bnds'][:] = time_bnds
    tar_nc.variables['depth_bnds'][:] = depth_bnds
    tar_nc.variables['time'].units = time_units
    tar_nc.variables['lat'].units = "degrees_N"
    tar_nc.variables['lon'].units = "degrees_E"

    DstFilePath = '/home/zju/do4/soda342/soda3.4.2_mn_ocean_reg_{0}.nc'.format(str(year))
    X = Dataset(DstFilePath)
    lon_min = 21.75
    lon_max = 151.25
    lat_min = -49.75
    lat_max = 26.75
    lon = X.variables['xt_ocean'][:]
    lat = X.variables['yt_ocean'][:]
    lon_min_index = np.where(lon == lon_min)[0][0]
    lon_max_index = np.where(lon == lon_max)[0][0]
    lat_min_index = np.where(lat == lat_min)[0][0]
    lat_max_index = np.where(lat == lat_max)[0][0]

    taux = X.variables['taux'][:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    tauy = X.variables['tauy'][:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    ssh = X.variables['ssh'][:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    u = X.variables['u'][:,:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    v = X.variables['v'][:,:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    w = X.variables['wt'][:,:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    salt = X.variables['salt'][:,:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    temp = X.variables['temp'][:,:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    mlp = X.variables['mlp'][:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    mls = X.variables['mls'][:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    mlt = X.variables['mlt'][:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    net_heating = X.variables['net_heating'][:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    prho = X.variables['prho'][:,:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    
    region_path = '/home/zju/do5/predict/code/sea_num_soda342.nc'
    sea_region_nc = Dataset(region_path)

    sea_region = sea_region_nc.variables['sea_num'][lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    sea_region_ravel = sea_region.ravel()
    sea_region_list = (sea_region_ravel != 14) & (sea_region_ravel != 15) & (sea_region_ravel != 16) & (sea_region_ravel != 17) & (sea_region_ravel != 18) & (sea_region_ravel != 19)

    for monthIndex in range(0, 12):
        for depthIndex in range(0, 50):
            _u = u[monthIndex, depthIndex, :].ravel()
            _v = v[monthIndex, depthIndex, :].ravel()
            _w = w[monthIndex, depthIndex, :].ravel()
            _temp = temp[monthIndex, depthIndex, :].ravel()
            _salt = salt[monthIndex, depthIndex, :].ravel()
            _ssh = ssh[monthIndex, :].ravel()
            _taux = taux[monthIndex, :].ravel()
            _tauy = tauy[monthIndex, :].ravel()
            _mlp = mlp[monthIndex, :].ravel()
            _mls = mls[monthIndex, :].ravel()
            _mlt = mlt[monthIndex, :].ravel()
            _net_heating = net_heating[monthIndex, :].ravel()
            _prho = prho[monthIndex, depthIndex, :].ravel()

            X_mask_data = np.c_[_u, _v, _w, _temp, _salt, _ssh, _taux, _tauy, _mlp, _mls, _mlt, _net_heating, _prho]
            mask_all = np.matrix(X_mask_data < -1000000)
            mask_all = np.c_[mask_all, sea_region_list]
            mask_mul = np.matrix([True, True, True, True, True, True, True, True, True, True, True, True, True, True]).T
            mask = np.matmul(np.array(mask_all), mask_mul)

            judge_index = np.where(mask == False)
            if len(judge_index[0]) == 0:
                continue
            
            y_pred = o2[monthIndex, depthIndex, :].ravel()
            y_pred = ma.masked_array(y_pred, mask=mask)
            y_pred = y_pred.reshape(lat_Indian.shape[0], lon_Indian.shape[0])
            tar_nc.variables['o2'][monthIndex, depthIndex, :, :] = y_pred


    src_nc.close()
    tar_nc.close()
    sea_region_nc.close()
    X.close()
    return


if __name__ == '__main__':

    items = [i for i in range(1981,2020)]
    #items = [i for i in range(1980,1981)]
    pool = Pool(40)
    pool.map(process, items)
    pool.close()
    pool.join() 
    
