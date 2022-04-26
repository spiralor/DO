import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

"""
@File:      f4DataAnalyze.py
@Author:    HuangSheng
@Time:      2022/1/23   
@Description:   分析GRIDDING_2db_f4_seed1_after1970数据分布
"""

# f4_data_path = 'GRIDDING_2db_f4_seed1_after1970.csv'
f4_data_path =  'GRIDDING_2dbf4_new_all_after1970_1000_max.csv'
# f4_data_path = 'GRIDDING_2dbf4_new_seed1_after1970.csv'

title = f4_data_path[:-4]
if __name__ == '__main__':
    f4_data_pd = pd.read_csv(f4_data_path, engine='python')
    col = 'Oxy_median'
    f4_data = f4_data_pd[col]
    print('f4_data size', len(f4_data))

    data_distribute = {}
    threshold = 5
    min_scale = 0
    max_scale = min_scale + threshold
    scale_key = str(min_scale) + ' to ' + str(max_scale)
    seek_index = np.where((f4_data >= min_scale) & (f4_data < max_scale))[0]

    print('seek index:', len(seek_index))

    while len(seek_index) != 0:
        data_distribute[scale_key] = len(seek_index) / len(f4_data)
        # data_distribute[scale_key] = ((- (len(seek_index) / len(f4_data)) + 0.05) * 1000) ** 2
        min_scale = min_scale + threshold
        max_scale = max_scale + threshold
        scale_key = str(min_scale) + ' to ' + str(max_scale)
        seek_index = np.where((f4_data >= min_scale) & (f4_data < max_scale))[0]

    y = []
    x = []
    n = 0
    sum = 0
    for key in data_distribute.keys():
        print(key, ':', data_distribute[key])
        y.append(data_distribute[key])
        x.append(n)
        n += 1
        sum += data_distribute[key]
    y = np.array(y)
    x = np.array(x)
    print('sum: ', sum)
    plt.title(title)
    rect = plt.bar(x=x, height=y)
    plt.show()

    df = pd.DataFrame(columns=['range', 'sample_weight'])
    for key in data_distribute.keys():
        df = df.append(pd.Series({'range': key, 'sample_weight': data_distribute[key]}, name=key))
    df.to_csv('sample_weight.csv', index=None)
