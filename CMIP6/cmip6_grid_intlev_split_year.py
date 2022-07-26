import os

year_start = 1980
year_end = 1984

while year_start <= 2015:
    src_path = "MPI-ESM1-2-HR_grid_intlev/o2_MPI-ESM1-2-HR_{0}-{1}_grid_intlev.nc".format(str(year_start), str(year_end))
    
    for year in range(year_start, year_end+1):
        target_path = "MPI-ESM1-2-HR_grid_intlev/year/o2_MPI-ESM1-2-HR_{0}_grid_intlev.nc".format(year)
        cdo_cmd = "cdo -z zip selyear,{0} {1} {2}".format(str(year),src_path,target_path)
        #print(cdo_cmd)
        os.system(cdo_cmd)

    year_start += 5
    year_end   += 5
