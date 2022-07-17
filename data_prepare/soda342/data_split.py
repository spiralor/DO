import numpy as np
import pandas as pd
import sklearn
from sklearn.model_selection import train_test_split, KFold

csv_path = 'Y:/do_train/Indian_region/dataset/GRIDDING_SODA342_2db_Indian.csv'
seed = 7
cv = 5

depthList = [5.034, 15.101, 25.219, 35.358, 45.576, 55.853, 66.262, 76.803, 87.577,
             98.623, 110.096, 122.107, 134.909, 148.747, 164.054, 181.312, 201.263,
             224.777, 253.068, 287.551, 330.008, 382.365, 446.726, 524.982, 618.703,
             728.692, 854.994, 996.715, 1152.376, 1319.997, 1497.562, 1683.057, 1874.788,
             2071.252, 2271.323, 2474.043, 2678.757, 2884.898, 3092.117, 3300.086, 3508.633,
             3717.567, 3926.813, 4136.251, 4345.864, 4555.566, 4765.369, 4975.209, 5185.111,
             5395.023]

def make_split_dataset():
    all_data_pd = pd.read_csv(csv_path)

    header = all_data_pd.keys()
    all_data = all_data_pd.values
    train_data = []
    valid_data = []

    kf = KFold(n_splits=cv, shuffle=True, random_state=seed)
    splits_index = kf.split(all_data)

    for train_index, val_index in splits_index:
        tmp_data = all_data[train_index]
        train_data.append(tmp_data)

        tmp_data = all_data[val_index]
        valid_data.append(tmp_data)

    # save_train_data = np.array(train_data[0])
    # save_data_path = 'Y:/do_train/Indian_region/dataset/GRIDDING_SODA342_2db_Indian_train_index0.csv'
    # save_train_data_pd = pd.DataFrame(save_train_data)
    # save_train_data_pd.to_csv(save_train_data_pd, header=header, index=False)

    save_valid_data = np.array(valid_data[0])
    save_valid_data_path = 'Y:/do_train/Indian_region/dataset/GRIDDING_SODA342_2db_Indian_test_index0.csv'
    save_data_pd = pd.DataFrame(save_valid_data)
    save_data_pd.to_csv(save_valid_data_path, header=header, index=False)
    return


def statics_split_dataset():
    """
    ['Year', 'Month', 'Longitude', 'Latitude', 'depth(m)', 'Oxy_mean',
       'Oxy_median', 'u', 'v', 'w', 'temp', 'salt', 'ssh', 'taux', 'tauy',
       'ETOPO1_Bed', 'o2_CMIP6', 'o2sat', 'station_id', 'station_count',
       'woa13_year', 'sea_num', 'mlp', 'mls', 'mlt', 'net_heating', 'prho',
       'time_id']
    """
    train_data_path = 'Y:/do_train/Indian_region/dataset/GRIDDING_SODA342_2db_Indian_train_index0.csv'
    train_data_path = 'Y:/do_train/Indian_region/dataset/GRIDDING_SODA342_2db_Indian_valid_index0.csv'
    train_data_pd = pd.read_csv(train_data_path)
    train_data = train_data_pd.values
    train_data_pd_keys = train_data_pd.keys()

    # for year in range(1980, 2020):
    #     # data_index = np.where(train_data[:, 0] == year)[0]
    #     data_index = np.where(train_data_pd == year)[0]
    #     print(len(data_index))
    #     # print(year, len(data_index))
    # return

    for depth in  depthList:
        data_index = np.where(train_data_pd == depth)[0]
        print(len(data_index))

    return


if __name__ == '__main__':
    # make_split_dataset()
    statics_split_dataset()
