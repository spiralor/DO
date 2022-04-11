import numpy as np
from netCDF4 import Dataset, num2date
import numpy.ma as ma


ocean_area = Dataset(r'../ocean_area_0.5deg.nc')
# variables = area_num
dict = {'Antarctic':20, 'Arctic':21, 'Equatorial_Atlantic':4, 'Equatorial_Indian':16,
        'Equatorial_Pacific':10, 'North_Atlantic':2, 'North_Pacific':8,
        'South_Atlantic':6, 'South_Indian':18, 'South_Pacific':12, 'North_Indian':14,
        'Coastal_N_Indian':15,'Coastal_Eq_Indian':17,'Coastal_S_Indian':19}

# -75.5 ~ 88.5 度
"""
算体积
"""
def create_ocean_region_nc(year,ocean_region,file_path):

    global_ocean_nc = Dataset(
        r'../' + file_path + '/{0}_do_predict_timmean.nc'.format(str(year)))
    time = global_ocean_nc.variables['time'][:]
    depth = global_ocean_nc.variables['depth'][:]
    lat = global_ocean_nc.variables['lat'][:]
    lon = global_ocean_nc.variables['lon'][:]
    o2 = global_ocean_nc.variables['o2_pred_rf'][:,:,:,:]

    region_ocean_nc = Dataset(
        r'../' + file_path + '/o2_{0}_{1}.nc'.format(str(year),ocean_region), 'w', format='NETCDF4')
    region_ocean_nc.createDimension('time', 1)
    region_ocean_nc.createDimension('depth', 40)
    region_ocean_nc.createDimension('lat', 330)
    region_ocean_nc.createDimension('lon', 720)
    region_ocean_nc.createVariable('o2_pred_rf', 'f', ('time', 'depth', 'lat', 'lon'), fill_value=-9.99E33)
    region_ocean_nc.createVariable('time', 'd', ('time'))
    region_ocean_nc.createVariable('depth', 'd', ('depth'))
    region_ocean_nc.createVariable('lat', 'd', ('lat'))
    region_ocean_nc.createVariable('lon', 'd', ('lon'))
    region_ocean_nc.variables['time'][:] = time
    region_ocean_nc.variables['depth'][:] = depth
    region_ocean_nc.variables['lat'][:] = lat
    region_ocean_nc.variables['lon'][:] = lon
    region_ocean_nc.variables['lat'].units = "degrees_north"
    region_ocean_nc.variables['lon'].units = "degrees_east"
    level_mask = np.matrix( ocean_area.variables['area_num'][29:359,:] != dict[ocean_region])

    for depthIndex in range(40):
        area_o2 = ma.masked_array(o2[0,depthIndex,:,:], mask=level_mask)
        region_ocean_nc.variables['o2_pred_rf'][0,depthIndex,:,:] = area_o2

    global_ocean_nc.close()
    region_ocean_nc.close()

# 海区分区计算
def create_region_area_nc(key, file_path=None):
    grid_area = Dataset(r'../gr_area.nc')
    ocean_area = Dataset(r'../ocean_area_0.5deg.nc')

    region_grid_area = Dataset(
        r'../ocean_area/{0}_area.nc'.format(key), 'w', format='NETCDF4')
    region_grid_area.createDimension('lat', 330)
    region_grid_area.createDimension('lon', 720)
    region_grid_area.createVariable('cell_area', 'f', ('lat', 'lon'), fill_value=-9.99E33)
    region_grid_area.createVariable('lat', 'd', ('lat'))
    region_grid_area.createVariable('lon', 'd', ('lon'))
    region_grid_area.variables['lat'][:] = grid_area.variables['latitude'][:]
    region_grid_area.variables['lon'][:] = grid_area.variables['longitude'][:]
    region_grid_area.variables['lat'].units = "degrees_north"
    region_grid_area.variables['lon'].units = "degrees_east"
    region_mask = np.matrix(ocean_area.variables['area_num'][29:359,:] != dict[key])
    cell_grid = ma.masked_array(grid_area.variables['cell_area'][:], mask=region_mask)
    region_grid_area.variables['cell_area'][:,:] = cell_grid

    grid_area.close()
    region_grid_area.close()


if __name__ == '__main__':

    file_path = 'cdo_RF_500_15_fit_1970_qc_third_o2_median_delete_lon/change_rate'
    for key in dict.keys():
        create_region_area_nc(key, file_path)
        # for year in [1960, 1970, 1980, 1990, 2000, 2010]:
        #     create_ocean_region_nc(year, key, file_path)
        print(key + ' finish!')

    # 计算DO水柱的总体积
    shell = "cdo -vertsum -expr,'V = o2_pred_rf/1000/1000/1000/1000000'  -mul -fldsum -mul " \
            "-setrtoc,0,1000,1 -timmean ../ExtraTreesRegressor/RESULT/ensemble/mean/1871_do_predict_mean.nc " \
            "../gr_area.nc ../depth_height.nc ../ocean_area/Total_volume.nc "
    os.system(shell)
    # 计算DO不同大洋上水柱的总体积
    for key in dict.keys():
        shell = "cdo -vertsum -expr,'V = o2_pred_rf/1000/1000/1000/1000000'  -mul -fldsum -mul " \
            "-setrtoc,0,1000,1 -timmean ../ExtraTreesRegressor/RESULT/ensemble/mean/1871_do_predict_mean.nc " \
            "../ocean_area/{0}_area.nc ../depth_height.nc ../ocean_area/{0}_volume.nc ".format(key)
        os.system(shell)

        
