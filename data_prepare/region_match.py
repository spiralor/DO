import numpy as np
import pandas as pd
from netCDF4 import Dataset
import os
from multiprocessing import Pool

nc_path = '../../do_predict/predict/ocean_area_0.5deg.nc'
nc = Dataset(nc_path)
region = nc.variables['area_num'][:]

sea_num_dict = {'Antarctic':20, 'Arctic':21, 'Equatorial_Atlantic':4, 'Equatorial_Indian':16,
        'Equatorial_Pacific':10, 'North_Atlantic':2, 'North_Pacific':8,
        'South_Atlantic':6, 'South_Indian':18, 'South_Pacific':12, 'North_Indian':14}

def match_sea_num(file_path):
    
    file_name = os.path.split(file_path)[-1][:-4]
    target_path = root_path  + file_name + '_sea_num.csv'

    all_data = pd.read_csv(file_path)
    lon = all_data['Longitude'].values
    lat = all_data['Latitude'].values
    lon_list = ((lon - 0.25) * 2).astype(int)
    lat_list = ((lat + 89.75) * 2).astype(int)
    num_list = region[lat_list, lon_list]

    all_data['sea_num'] = num_list
    all_data.to_csv(target_path, index=False, sep=',')
    print(target_path, 'save success!')
    return target_path

def export_sea_region_data(file_path):

    file_name = os.path.split(file_path)[-1][:-4]
    target_path = root_path  + file_name + '_Indian.csv'
    #target_path = root_path  + file_name + '_Indian_Nearshore.csv'
    #target_path = root_path  + file_name + '_Indian_Offshore.csv'
    all_data = pd.read_csv(file_path)
    lon_col = 'Longitude'
    lat_col = 'Latitude'
    depth = 'depth(m)'
    year_col = 'Year'
    month_col = 'Month'
    col_data_x = [year_col, month_col, lon_col, lat_col, depth, 'u', 'v', 'w', 'temp', 'salt', 'ssh', 'taux', 'tauy', 'ETOPO1_Bed', 'o2_CMIP6', 'sea_num']
    col_data_y = ['Oxy_median']
    all_data_x = all_data[col_data_x]
    all_data_y = all_data[col_data_y]
    all_data_y = all_data_y.values
    all_data_x = all_data_x.values
    all_data_y = all_data_y.squeeze()
    #Indian
    select_index = np.where((all_data_x[:, -1] == 14) | (all_data_x[:, -1] == 15) | (all_data_x[:, -1] == 16) | (all_data_x[:, -1] == 17) | 
                            (all_data_x[:, -1] == 18) | (all_data_x[:, -1] == 19))
    
    #select_index = np.where((all_data_x[:, -1] == 15) | (all_data_x[:, -1] == 17) | (all_data_x[:, -1] == 19))
    #select_index = np.where((all_data_x[:, -1] == 14) | (all_data_x[:, -1] == 16) | (all_data_x[:, -1] == 18))
    #Pacific
    #select_index = np.where((all_data_x[:, -1] == 8) | (all_data_x[:, -1] == 9) | (all_data_x[:, -1] == 10) | (all_data_x[:, -1] == 11) |
                            #(all_data_x[:, -1] == 12) | (all_data_x[:, -1] == 13))

    #Arctic
    #select_index = np.where(all_data_x[:, -1] == 21)
    
    train_data_x = all_data_x[select_index, :-1].reshape(-1, 15)
    train_data_y = all_data_y[select_index]
    print(target_path,'sample num:',len(train_data_y))
    col_data_x = [year_col, month_col, lon_col, lat_col, depth, 'u', 'v', 'w', 'temp', 'salt', 'ssh', 'taux', 'tauy', 'ETOPO1_Bed', 'o2_CMIP6', 'Oxy_median']

    excel = pd.DataFrame(np.c_[train_data_x, train_data_y])
    excel.to_csv(target_path, header=col_data_x, index=False)
    print(target_path, 'save success!')
    return


if __name__ == '__main__':
    
    db = ['2dbf5_t']

    for db_item in db:
        print(db_item, 'start')
        root_path =  '~/do4/do_train/dataset/2db/{0}/'.format(db_item)
        #file_path = [root_path + 'GRIDDING_{0}_seed1_after1970.csv'.format(db_item),
                     #root_path + 'GRIDDING_{0}_seed1_before1970.csv'.format(db_item),
                     #root_path + 'GRIDDING_{0}_seed1_other.csv'.format(db_item),
                     #root_path + 'GRIDDING_{0}_seed1_balance.csv'.format(db_item)]
        
        file_path = [root_path + 'GRIDDING_{0}.csv'.format(db_item)]
        #pool = Pool()
        #target_path = pool.map(match_sea_num, file_path)
        #pool.close()
        #pool.join()

        pool = Pool()
        pool.map(export_sea_region_data, file_path)
        pool.close()
        pool.join()
        print(db_item, 'finish')
