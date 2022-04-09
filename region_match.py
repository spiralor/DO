import numpy as np
import pandas as pd
from netCDF4 import Dataset

nc_path = 'C:/Users/26982/Desktop/DO Experiment/ocean_area_0.5deg.nc'
nc = Dataset(nc_path)
region = nc.variables['area_num'][:]
file_path = 'C:/Users/26982/Desktop/DO Experiment/GRIDDING_2dbnew_o2sat_seed1_after1970.csv'
all_data = pd.read_csv(file_path)
# lon = all_data['Longitude'].values
# lat = all_data['Latitude'].values
# lon_list = ((lon - 0.25) * 2).astype(int)
# lat_list = ((lat + 89.75) * 2).astype(int)
# print(lon_list)
# print(lat_list)
# num_list = region[lat_list, lon_list]
# train_csv['sea_num'] = num_list
# train_csv.to_csv('test3.csv', index=False, sep=',')

lon_col = 'Longitude'
lat_col = 'Latitude'
depth = 'depth(m)'
year_col = 'Year'
month_col = 'Month'

col_data_x = [lon_col, lat_col, year_col, month_col, depth, 'u', 'v', 'w', 'temp', 'salt', 'ssh', 'taux', 'tauy', 'ETOPO1_Bed', 'o2_CMIP6','sea_num']
col_data_y = ['Oxy_median']
all_data_x = all_data[col_data_x]
all_data_y = all_data[col_data_y]

x_min = all_data_x.min()
x_max = all_data_x.max()
all_data_y = all_data_y.values
y_min = np.min(all_data_y)
y_max = np.max(all_data_y)
all_data_x = all_data_x.values
all_data_y = all_data_y.squeeze()

all_data_x[:, 0] = np.cos(np.pi / 180 * all_data_x[:, 0])
train_data_x = all_data_x
train_data_y = all_data_y
select_index = np.where((all_data_x[:, -1] == 14) | (all_data_x[:, -1] == 16) | (all_data_x[:, -1] == 18))
train_data_x = all_data_x[select_index,:-1].reshape(-1,15)
train_data_y = all_data_y[select_index]

print(train_data_x.shape)
print(train_data_y.shape)



