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

etop_data = imageio.imread(r'F:\DO\ETOP\ETOPO1_Bed_resample_geotiff.tif')
sea_num_nc = Dataset(r'E:/do4/sea_region/ocean_area_0.5deg.nc')
sea_num_data = sea_num_nc.variables['area_num'][:]
etop_arr = np.array(etop_data)
etop_arr = etop_arr[::-1]
sea_num_arr = np.array(sea_num_data)

etop_nc_out_path = 'E:/do4/etop_soda342.nc'
etop_nc = Dataset(etop_nc_out_path, 'w', format='NETCDF4')
etop_nc.createDimension('lat', lat_dimension)
etop_nc.createDimension('lon', lon_dimension)
etop_nc.createVariable('lat', 'f', ('lat'), fill_value=-9.99E33, zlib=True)
etop_nc.createVariable('lon', 'f', ('lon'), fill_value=-9.99E33, zlib=True)
etop_nc.createVariable('etop', 'f', ('lat', 'lon'), fill_value=-9.99E33, zlib=True)
etop_lon_min = -179.75
etop_lon_max = 179.75
etop_lat_min = -89.75
etop_lat_max = 89.75


sea_region_out_path = 'E:/do4/sea_num_soda342.nc'
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

for lat_index in range(lat_dimension):
    for lon_index in range(lon_dimension):
        print(lat_index, lon_index)
        lon_value = lon_min + lon_index * lon_resolution
        lat_value = lat_min + lat_index * lat_resolution
        etop_nc.variables['lat'][lat_index] = lat_value
        etop_nc.variables['lon'][lon_index] = lon_value
        sea_region_nc.variables['lat'][lat_index] = lat_value
        sea_region_nc.variables['lon'][lon_index] = lon_value

        if lon_value >= 180:
            etop_lon_index = int((lon_value - 360 - etop_lon_min) / lon_resolution)
        elif lon_value < 180:
            etop_lon_index = int((lon_value - etop_lon_min) / lon_resolution)
        etop_lat_index = int((lat_value - etop_lat_min) / lat_resolution)
        etop_nc.variables['etop'][lat_index, lon_index] = etop_arr[etop_lat_index, etop_lon_index]

        sea_num_lat_index = int((lat_value - sea_region_lat_min) / lat_resolution)
        sea_num_lon_index = int((lon_value - sea_region_lon_min) / lon_resolution)
        sea_region_nc.variables['sea_num'][lat_index, lon_index] = sea_num_arr[sea_num_lat_index, sea_num_lon_index]




etop_nc.close()
sea_region_nc.close()
