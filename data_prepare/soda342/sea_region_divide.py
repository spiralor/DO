from netCDF4 import Dataset
import imageio
import numpy as np

"""
soda3.4.2
"""
lon_resolution = 0.5
lat_resolution = 0.5
lon_min = 0.25
lon_max = 359.75
lat_min = -74.75
lat_max = 89.75
lat_upper_bnds = int((lat_max - lat_min) / lat_resolution)
lon_upper_bnds = int((lon_max - lon_min) / lon_resolution)
lat_lower_bnds = 0
lon_lower_bnds = 0
lat_dimension = lat_upper_bnds + 1
lon_dimension = lon_upper_bnds + 1

sea_num_nc = Dataset(r'E:/do4/sea_region/ocean_area_0.5deg.nc')
sea_num_data = sea_num_nc.variables['area_num'][:]
sea_num_arr = np.array(sea_num_data)

"""
印度洋　14 15 16 17 18 19
"""

sea_region_out_path = 'E:/do4/sea_num_soda342_Indian.nc'
sea_region_nc = Dataset(sea_region_out_path, 'w', format='NETCDF4')
sea_region_nc.createDimension('lat', lat_dimension)
sea_region_nc.createDimension('lon', lon_dimension)
sea_region_nc.createVariable('lat', 'f', ('lat'), fill_value=-9.99E33, zlib=True)
sea_region_nc.createVariable('lon', 'f', ('lon'), fill_value=-9.99E33, zlib=True)
sea_region_nc.createVariable('sea_num', 'f', ('lat', 'lon'), fill_value=-9.99E33, zlib=True)
sea_region_lon_min = 0.25
sea_region_lon_max = 359.75
sea_region_lat_min = -89.75
sea_region_lat_max = 89.75

# sea_region_nc.variables['sea_num'][:] = np.full((lat_dimension, lon_dimension), np.nan)

for lat_index in range(lat_dimension):
    for lon_index in range(lon_dimension):
        lon_value = lon_min + lon_index * lon_resolution
        lat_value = lat_min + lat_index * lat_resolution
        sea_region_nc.variables['lat'][lat_index] = lat_value
        sea_region_nc.variables['lon'][lon_index] = lon_value
        sea_num_lat_index = int((lat_value - sea_region_lat_min) / lat_resolution)
        sea_num_lon_index = int((lon_value - sea_region_lon_min) / lon_resolution)
        flag = sea_num_arr[sea_num_lat_index, sea_num_lon_index]

        if flag == 14 or flag == 15 or flag == 16 or flag == 17 or flag == 18 or flag == 19:
            print(lat_index, lon_index, 'flag')
            sea_region_nc.variables['sea_num'][lat_index, lon_index] = flag
        else:
            print(lat_index, lon_index, 'nan')
            sea_region_nc.variables['sea_num'][lat_index, lon_index] = np.nan

sea_region_nc.close()
