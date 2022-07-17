"""
Time: 2022/7/17
Author: Huang Sheng
Description: 创建训练集，验证集
"""

import numpy as np
import pandas as pd
import sklearn
from sklearn.model_selection import train_test_split, KFold

csv_path = 'Y:/do_train/Indian_region/dataset/GRIDDING_SODA342_2db_Indian.csv'
seed = 7
cv = 5

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
save_valid_data_path = 'Y:/do_train/Indian_region/dataset/GRIDDING_SODA342_2db_Indian_valid_index0.csv'
save_data_pd = pd.DataFrame(save_valid_data)
save_data_pd.to_csv(save_valid_data_path, header=header, index=False)
