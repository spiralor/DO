from netCDF4 import Dataset
import numpy.ma as ma
import numpy as np

"""
soda3.4.2 Inidan range
"""
lon_resolution = 0.5
lat_resolution = 0.5
lon_min = 21.75
lon_max = 151.25
lat_min = -49.75
lat_max = 26.75
lat_upper_bnds = int((lat_max - lat_min) / lat_resolution)
lon_upper_bnds = int((lon_max - lon_min) / lon_resolution)
lat_lower_bnds = 0
lon_lower_bnds = 0
lat_dimension = lat_upper_bnds + 1
lon_dimension = lon_upper_bnds + 1

gr_area_Indian_nc = Dataset('Y:/gr_area_soda342_Indian.nc')
gr_area_lon = gr_area_Indian_nc.variables['xt_ocean'][:]
gr_area_lat = gr_area_Indian_nc.variables['yt_ocean'][:]
gr_area_Indian = gr_area_Indian_nc.variables['cell_area'][:]

sea_num_Indian_nc = Dataset('Y:/sea_num_soda342_Indian.nc')
sea_num_lon = sea_num_Indian_nc.variables['lon'][:]
sea_num_lat = sea_num_Indian_nc.variables['lat'][:]

lat_min_index = np.where(sea_num_lat == lat_min)[0][0]
lat_max_index = np.where(sea_num_lat == lat_max)[0][0] + 1
lon_min_index = np.where(sea_num_lon == lon_min)[0][0]
lon_max_index = np.where(sea_num_lon == lon_max)[0][0] + 1

sea_num_Indian = sea_num_Indian_nc.variables['sea_num'][lat_min_index:lat_max_index, lon_min_index:lon_max_index]
sea_num_Indian = np.array(sea_num_Indian)

full_depth_height_Indian_nc = Dataset('Y:/soda342_full_depth_height_Indian.nc')
full_depth_height_Indian = full_depth_height_Indian_nc.variables['depth_height'][:]

"""
印度洋　14 15 16 17 18 19
"""
select_num = 19

# sea_region_out_path = 'Y:/gr_area_soda342_Indian_{0}.nc'.format(str(select_num))
# sea_region_nc = Dataset(sea_region_out_path, 'w', format='NETCDF4')
# sea_region_nc.createDimension('lat', lat_dimension)
# sea_region_nc.createDimension('lon', lon_dimension)
# sea_region_nc.createVariable('lat', 'f', ('lat'), fill_value=-9.99E33, zlib=True)
# sea_region_nc.createVariable('lon', 'f', ('lon'), fill_value=-9.99E33, zlib=True)
# sea_region_nc.createVariable('cell_area', 'f', ('lat', 'lon'), fill_value=-9.99E33, zlib=True)
#
# sea_region_nc.variables['lon'][:] = gr_area_lon
# sea_region_nc.variables['lat'][:] = gr_area_lat
#
# gr_area_Indian_select = np.array(gr_area_Indian)
# mask = (sea_num_Indian != select_num)
# gr_area_Indian_select = ma.masked_array(gr_area_Indian_select, mask=mask)
# sea_region_nc.variables['cell_area'][:] = gr_area_Indian_select
#
# sea_region_nc.close()


sea_depth_out_path = 'Y:/soda342_full_depth_height_Indian_{0}.nc'.format(str(select_num))
sea_depth_nc = Dataset(sea_depth_out_path, 'w', format='NETCDF4')
sea_depth_nc.createDimension('lat', lat_dimension)
sea_depth_nc.createDimension('lon', lon_dimension)
sea_depth_nc.createDimension('depth', 50)
sea_depth_nc.createVariable('lat', 'f', ('lat'), fill_value=-9.99E33, zlib=True)
sea_depth_nc.createVariable('lon', 'f', ('lon'), fill_value=-9.99E33, zlib=True)
# sea_depth_nc.createVariable('depth', 'f', ('depth'), fill_value=-9.99E33, zlib=True)
sea_depth_nc.createVariable('depth_height', 'f', ('depth', 'lat', 'lon'), fill_value=-9.99E33, zlib=True)

sea_depth_nc.variables['lon'][:] = gr_area_lon
sea_depth_nc.variables['lat'][:] = gr_area_lat

depth_Indian_select = np.array(full_depth_height_Indian)
mask = (sea_num_Indian != select_num)

for depth_index in range(50):
    tmp = ma.masked_array(depth_Indian_select[depth_index, :], mask=mask)
    sea_depth_nc.variables['depth_height'][depth_index, :] = tmp

sea_depth_nc.close()
