from netCDF4 import Dataset
import numpy as np
import os


"""
dict_keys(['time', 'xt_ocean', 'yt_ocean', 'xu_ocean', 'yu_ocean', 'st_ocean', 'sw_ocean', 'temp', 'salt', 'wt', 'ssh', 'mlt', 'mlp', 'mls', 'net_heating', 'prho', 'u', 'v', 'taux', 'tauy'])

5.034

"""
depthList = "6,15.101,25.219,35.358,45.576,55.853,66.262,76.803,87.577,98.623,110.096,122.107,134.909,148.747,164.054,181.312,201.263,224.777,253.068,287.551,330.008,382.365,446.726,524.982,618.703,728.692,854.994,996.715,1152.376,1319.997,1497.562,1683.057,1874.788,2071.252,2271.323,2474.043,2678.757,2884.898,3092.117,3300.086,3508.633,3717.567,3926.813,4136.251,4345.864,4555.566,4765.369,4975.209,5185.111,5395.023"

year_start = 1980
year_end = 1984
file_name = "o2_Omon_MPI-ESM1-2-HR_historical_r1i1p1f1_gn_185001-185412.nc"

while year_start <= 2010:
	
	is_exists = False
	file_path = "MPI-ESM1-2-HR/o2_Omon_MPI-ESM1-2-HR_historical_r1i1p1f1_gn_{0}01-{1}12.nc".format(str(year_start), str(year_end))
	target_path = "MPI-ESM1-2-HR_grid_intlev/o2_MPI-ESM1-2-HR_{0}-{1}_grid_intlev.nc".format(str(year_start), str(year_end))
	year_start = year_start + 5
	year_end = year_end + 5
	
	#if os.path.exists(file_path):
	#	is_exists = True
	#print(file_path, is_exists)
	cdo_cmd = "cdo -z zip -genlevelbounds -chlevel,6,5.034 -intlevel,{0} -remapdis,r720x360 {1} {2}".format(depthList,file_path,target_path)
	#os.system(cdo_cmd)
	#print(cdo_cmd)
	


file_path   = "MPI-ESM1-2-HR/o2_Omon_MPI-ESM1-2-HR_ssp585_r1i1p1f1_gn_201501-201912.nc" 
target_path = "MPI-ESM1-2-HR_grid_intlev/o2_MPI-ESM1-2-HR_2015-2019_grid_intlev.nc"		 
cdo_cmd = "cdo -z zip -genlevelbounds -chlevel,6,5.034 -intlevel,{0} -remapdis,r720x360 {1} {2}".format(depthList,file_path,target_path)
os.system(cdo_cmd)
