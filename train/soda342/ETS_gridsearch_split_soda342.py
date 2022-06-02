# K-fold cross validation methods to find best params

from sklearn.model_selection import RandomizedSearchCV
from sklearn.model_selection import cross_val_score, GridSearchCV, cross_val_predict
from sklearn.ensemble import RandomForestRegressor, ExtraTreesRegressor, GradientBoostingRegressor
from sklearn import model_selection
import numpy as np
import pandas as pd
import datetime
import joblib
import os
from scipy.stats.stats import pearsonr
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
from sklearn.model_selection import train_test_split, KFold
import time as tm
seed = 7
cv = 5


def read_data(all_data_path):
    all_data = pd.read_csv(all_data_path, engine='python')
    col_data_x = ['Longitude','Latitude','Year','Month','depth(m)','u','v','w','temp','salt','ssh','taux','tauy',
                  'ETOPO1_Bed','mlp','mls','mlt','net_heating','prho']
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
    train_data_x = all_data_x
    train_data_y = all_data_y

    return train_data_x, train_data_y, x_min, x_max, y_min, y_max, col_data_x


def read_split_data(all_data_path):
    all_data = pd.read_csv(all_data_path, engine='python')
    col_data_x = ['Longitude','Latitude','Year','Month','depth(m)','u','v','w','temp','salt','ssh','taux','tauy',
                  'ETOPO1_Bed','mlp','mls','mlt','net_heating','prho']
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

    train_data_x = []
    train_data_y = []
    
    valid_data_x = []
    valid_data_y = []

    kf = KFold(n_splits=cv, shuffle=True, random_state=seed)
    splits_index = kf.split(all_data_x)
    for train_index, val_index in splits_index:
        tmp_data_x = all_data_x[train_index]
        tmp_data_y = all_data_y[train_index]
        train_data_x.append(tmp_data_x)
        train_data_y.append(tmp_data_y)

        tmp_data_x = all_data_x[val_index]
        tmp_data_y = all_data_y[val_index]
        valid_data_x.append(tmp_data_x)
        valid_data_y.append(tmp_data_y)
    
    return train_data_x, train_data_y, valid_data_x, valid_data_y, x_min, x_max, y_min, y_max, col_data_x

def cal_statistic(y_real, y_pred):
    con = ~np.isnan(y_pred) & np.isfinite(y_pred)
    y_real = y_real[con]
    y_pred = y_pred[con]
    R2 = r2_score(y_real, y_pred)
    RMSE = np.sqrt(mean_squared_error(y_real, y_pred))
    MAE = mean_absolute_error(y_real, y_pred)
    ape = np.abs((y_real - y_pred) / y_real)
    con = ~np.isnan(ape) & np.isfinite(ape)
    MAPE = np.mean(ape[con])
    r = pearsonr(y_real.squeeze(), y_pred.squeeze())[0]

    return [R2, RMSE, MAE, MAPE, r]

def cal_test_stats(X, Y, model):
    y_pred = model.predict(X).squeeze()
    y_real = Y
    count = len(y_real)
    stats = cal_statistic(y_real, y_pred)
    return stats[0], stats[1], stats[2], stats[3], stats[4],count

def train(seed, all_data_path, file_name):
    
    print('file_name = ', file_name, ', time = ',str(tm.strftime("%Y-%m-%d %H:%M:%S", tm.localtime())))

    excel = pd.DataFrame(columns=['count', 'R2', 'RMSE', 'MAE', 'MAPE', 'r'])
    best_RMSE = 9999
    best_MAE  = 9999
    best_MAPE = 9999
    best_R2 = -1
    best_r = -1
    best_n_estimators = -1
    best_max_depth = -1
    best_split = -1
    
    # 空间CV划分数据集的gridsearch
    for n_estimators in [200,300,400]: # 200,300,400   
        for max_depth in [15,20,25,30,35,40,None]: # 15,20,25,30,35,40,None 
            for min_samples_split in [2,5,10]: #2,5,10
                print('n_estimators = ', n_estimators, ', max_depth = ', max_depth, ', min_sample_split = ', 
                        min_samples_split,', time = ',str(tm.strftime("%Y-%m-%d %H:%M:%S", tm.localtime()))) 
                predict_list =[]
                real_list = []

                for index in range(cv):
                    model_estimate=ExtraTreesRegressor(random_state=seed, n_jobs=-1,n_estimators=n_estimators,max_depth=max_depth,min_samples_split=min_samples_split)
                    model_estimate.fit(train_X[index], train_Y[index])
                    cal_R2, cal_RMSE, cal_MAE, cal_MAPE, cal_r, count_r= cal_test_stats(valid_X[index], valid_Y[index], model_estimate)    
                    excel = excel.append( pd.Series({'count':count_r,'R2':cal_R2, 'RMSE':cal_RMSE , 'MAE':cal_MAE , 'MAPE':cal_MAPE , 'r':cal_r }, name= str(index)))       
                    predict_list.extend(model_estimate.predict(valid_X[index]).squeeze().ravel())
                    real_list.extend(valid_Y[index].ravel())

                real_listn = np.array(real_list).ravel()
                predict_listn = np.array(predict_list).ravel()
                R2, RMSE, MAE, MAPE, r = cal_statistic(real_listn, predict_listn)
                excel = excel.append( pd.Series({'count':len(real_listn),'R2': R2, 'RMSE': RMSE, 'MAE': MAE, 'MAPE': MAPE, 'r': r},
                    name='depth_' + str(n_estimators)+ ', ' + str(max_depth) + ', ' + str(min_samples_split)))

                lt20_index = np.where(real_listn <= 20)
                real_list_lt20 = real_listn[lt20_index]
                predict_list_lt20 = predict_listn[lt20_index]
                R21, RMSE1, MAE1, MAPE1, r1 = cal_statistic(real_list_lt20, predict_list_lt20)
                excel = excel.append( pd.Series({'count':len(real_list_lt20),'R2': R21, 'RMSE': RMSE1, 'MAE': MAE1, 'MAPE': MAPE1, 'r': r1},
                    name='lt20_depth_' + str(n_estimators)+ ', ' + str(max_depth) + ', ' + str(min_samples_split)))

                if best_MAE > MAE:
                    best_RMSE = RMSE
                    best_MAE = MAE
                    best_MAPE = MAPE
                    best_R2 = R2
                    best_r = r
                    best_n_estimators = n_estimators
                    best_max_depth = max_depth
                    best_split = min_samples_split

    # 输出最优结果和超参，建立最优模型并且保存
    excel = excel.append(pd.Series({'count':len(np.array(real_list).ravel()),'R2': best_R2, 'RMSE': best_RMSE, 'MAE': best_MAE, 'MAPE': best_MAPE, 'r': best_r}, name='best,'+str(best_n_estimators)+','+str(best_max_depth)+','+str(best_split)))
                
    best_estimate=ExtraTreesRegressor(random_state=seed, n_jobs=-1,n_estimators=best_n_estimators,max_depth=best_max_depth,min_samples_split=best_split)
    best_estimate.fit(test_X, test_Y)
    #joblib.dump(best_estimate, '/home/zju/do5/model/' + file_name + '.pkl')

    # 指标验证
    cal_R2, cal_RMSE, cal_MAE, cal_MAPE, cal_r, count_r = cal_test_stats(test_X, test_Y, best_estimate)
    excel = excel.append( pd.Series({'count':count_r,'R2':cal_R2, 'RMSE':cal_RMSE , 'MAE':cal_MAE , 'MAPE':cal_MAPE , 'r':cal_r }, name= 'test_all' ))
    
    excel.to_csv(file_name + '.csv')
    print('finish! time = ', str(tm.strftime("%Y-%m-%d %H:%M:%S", tm.localtime())))
    return

if __name__ == '__main__':

    all_data_path  = 'GRIDDING_SODA342_2db_Indian.csv'
    test_data_path = 'GRIDDING_SODA342_2db_Indian.csv'
    file_name = 'ets_fit_GRIDDING_SODA342_2db_Indian_in_best_params'

    train_X, train_Y, valid_X, valid_Y, x_min, x_max, y_min, y_max, col_data_x = read_split_data(all_data_path) 
    test_X, test_Y, x_min, x_max, y_min, y_max, col_x = read_data(test_data_path)

    train(seed, all_data_path, file_name)
