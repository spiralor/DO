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
from sklearn.model_selection import train_test_split
import time as tm

seed = 7
cv = 10


def read_data(all_data_path):
    all_data = pd.read_csv(all_data_path, engine='python')

    lon_col = 'Longitude'
    lat_col = 'Latitude'
    depth = 'depth(m)'
    year_col = 'Year'
    month_col = 'Month'

    col_data_x = [lon_col, lat_col, year_col, month_col, depth, 'u', 'v', 'w', 'temp', 'salt', 'ssh', 'taux', 'tauy',
                  'ETOPO1_Bed', 'o2_CMIP6']
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

    lon_col = 'Longitude'
    lat_col = 'Latitude'
    depth = 'depth(m)'
    year_col = 'Year'
    month_col = 'Month'
    col_data_x = [lon_col, lat_col, year_col, month_col, depth, 'u', 'v', 'w', 'temp', 'salt', 'ssh', 'taux', 'tauy',
                  'ETOPO1_Bed', 'o2_CMIP6']
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

    # 操作空间范围
    train_data_x = []
    train_data_y = []

    valid_data_x = []
    valid_data_y = []
    for index in range(cv):
        lon_min = 360 * index / cv
        lon_max = 360 * (index + 1) / cv

        select_index = np.where((all_data_x[:, 0] >= lon_min) & (all_data_x[:, 0] < lon_max))
        tmp_data_x = all_data_x[select_index]
        tmp_data_y = all_data_y[select_index]
        valid_data_x.append(tmp_data_x)
        valid_data_y.append(tmp_data_y)

        select_index = np.where((all_data_x[:, 0] < lon_min) | (all_data_x[:, 0] >= lon_max))
        tmp_data_x = all_data_x[select_index]
        tmp_data_y = all_data_y[select_index]
        train_data_x.append(tmp_data_x)
        train_data_y.append(tmp_data_y)

    return train_data_x, train_data_y, valid_data_x, valid_data_y, x_min, x_max, y_min, y_max, col_data_x


def cal_statistic(y_real, y_pred, X=None, result_name=None):
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

    if result_name != None:
        excel_result = pd.DataFrame(
            {'x_lon': X[:, 0], 'x_lat': X[:, 1], 'x_year': X[:, 2], 'x_month': X[:, 3], 'x_depth': X[:, 4],
             'x_u': X[:, 5], 'x_v': X[:, 6], 'x_w': X[:, 7],
             'x_temp': X[:, 8], 'x_salt': X[:, 9], 'x_ssh': X[:, 10], 'x_taux': X[:, 11], 'x_tauy': X[:, 12],
             'x_etop': X[:, 13], 'x_cmip': X[:, 14], 'y_real': y_real, 'y_pred': y_pred})
        excel_result.to_csv('validation/' + result_name + '.csv', index=False, sep=',')

    return [R2, RMSE, MAE, MAPE, r]


def cal_test_stats(X, Y, model, result_name=None):
    y_pred = model.predict(X).squeeze()
    y_real = Y
    count = len(y_real)
    stats = cal_statistic(y_real, y_pred, X, result_name)
    return stats[0], stats[1], stats[2], stats[3], stats[4], count


def cal_cmip_stats(X, model, result_name=None):
    y_pred = model.predict(X).squeeze()
    y_real = X[:, 14]
    count = len(y_real)
    stats = cal_statistic(y_real, y_pred, result_name=result_name)
    return stats[0], stats[1], stats[2], stats[3], stats[4], count


def train(seed, all_data_path, file_name):
    print('file_name = ', file_name, ', time = ', str(tm.strftime("%Y-%m-%d %H:%M:%S", tm.localtime())))

    excel = pd.DataFrame(columns=['count', 'R2', 'RMSE', 'MAE', 'MAPE', 'r'])
    best_RMSE = 9999
    best_MAE = 9999
    best_MAPE = 9999
    best_R2 = -1
    best_r = -1
    best_n_estimators = -1
    best_max_depth = -1

    # 空间CV划分数据集的gridsearch
    for n_estimators in [600]:
        for max_depth in [46]:
            print('n_estimators = ', n_estimators, ', max_depth = ', max_depth, ', time = ',
                  str(tm.strftime("%Y-%m-%d %H:%M:%S", tm.localtime())))
            predict_list = []
            real_list = []
            cmip_list = []
            predict_cvX_list = []
            for index in range(cv):
                model_estimate = ExtraTreesRegressor(random_state=seed, n_jobs=-1, n_estimators=n_estimators,
                                                     max_depth=max_depth)
                model_estimate.fit(train_X[index], train_Y[index])
                cal_R2, cal_RMSE, cal_MAE, cal_MAPE, cal_r, count_r = cal_test_stats(valid_X[index], valid_Y[index],
                                                                                     model_estimate)
                excel = excel.append(pd.Series(
                    {'count': count_r, 'R2': cal_R2, 'RMSE': cal_RMSE, 'MAE': cal_MAE, 'MAPE': cal_MAPE, 'r': cal_r},
                    name=str(index)))
                predict_list.extend(model_estimate.predict(valid_X[index]).squeeze().ravel())
                real_list.extend(valid_Y[index].ravel())
                cmip_list.extend(valid_X[index][:, 14].ravel())
                predict_cvX_list.extend(valid_X[index])
            predict_X_list = np.array(predict_cvX_list)
            # 保存预测-实测对比
            R2, RMSE, MAE, MAPE, r = cal_statistic(np.array(real_list).ravel(), np.array(predict_list).ravel(),
                                                   predict_X_list, result_name=file_name + '_pred-real_after1970')
            excel = excel.append(pd.Series(
                {'count': len(np.array(real_list).ravel()), 'R2': R2, 'RMSE': RMSE, 'MAE': MAE, 'MAPE': MAPE, 'r': r},
                name='pred-real after1970'))

            # 保存预测-模式对比
            R2, RMSE, MAE, MAPE, r = cal_statistic(np.array(cmip_list).ravel(), np.array(predict_list).ravel(),
                                                   result_name=None)
            excel = excel.append(pd.Series(
                {'count': len(np.array(real_list).ravel()), 'R2': R2, 'RMSE': RMSE, 'MAE': MAE, 'MAPE': MAPE, 'r': r},
                name='pred-cmip after1970'))

            if best_RMSE > RMSE:
                best_RMSE = RMSE
                best_MAE = MAE
                best_MAPE = MAPE
                best_R2 = R2
                best_r = r
                best_n_estimators = n_estimators
                best_max_depth = max_depth
    # 输出最优结果和超参，建立最优模型并且保存
    excel = excel.append(pd.Series(
        {'count': len(np.array(real_list).ravel()), 'R2': best_R2, 'RMSE': best_RMSE, 'MAE': best_MAE,
         'MAPE': best_MAPE, 'r': best_r}, name='best,' + str(best_n_estimators) + ',' + str(best_max_depth)))
    best_estimate = ExtraTreesRegressor(random_state=seed, n_jobs=-1, n_estimators=best_n_estimators,
                                        max_depth=best_max_depth)
    best_estimate.fit(test_X_after1970, test_Y_after1970)

    # 指标验证 预测-实测对比
    cal_R2, cal_RMSE, cal_MAE, cal_MAPE, cal_r, count_r = cal_test_stats(test_X_before1970, test_Y_before1970,
                                                                         best_estimate,
                                                                         result_name=file_name + '_pred-real_before1970')
    excel = excel.append(
        pd.Series({'count': count_r, 'R2': cal_R2, 'RMSE': cal_RMSE, 'MAE': cal_MAE, 'MAPE': cal_MAPE, 'r': cal_r},
                  name='pred-real_before1970'))
    cal_R2, cal_RMSE, cal_MAE, cal_MAPE, cal_r, count_r = cal_test_stats(test_X_other, test_Y_other, best_estimate,
                                                                         result_name=file_name + '_pred-real_other')
    excel = excel.append(
        pd.Series({'count': count_r, 'R2': cal_R2, 'RMSE': cal_RMSE, 'MAE': cal_MAE, 'MAPE': cal_MAPE, 'r': cal_r},
                  name='pred-real_other'))

    # 指标验证 预测-模式对比
    cal_R2, cal_RMSE, cal_MAE, cal_MAPE, cal_r, count_r = cal_cmip_stats(test_X_before1970, best_estimate,
                                                                         result_name=None)
    excel = excel.append(
        pd.Series({'count': count_r, 'R2': cal_R2, 'RMSE': cal_RMSE, 'MAE': cal_MAE, 'MAPE': cal_MAPE, 'r': cal_r},
                  name='pred-cmip_before1970'))
    cal_R2, cal_RMSE, cal_MAE, cal_MAPE, cal_r, count_r = cal_cmip_stats(test_X_other, best_estimate, result_name=None)
    excel = excel.append(
        pd.Series({'count': count_r, 'R2': cal_R2, 'RMSE': cal_RMSE, 'MAE': cal_MAE, 'MAPE': cal_MAPE, 'r': cal_r},
                  name='pred-cmip_other'))

    excel.to_csv('validation/' + file_name + '.csv')
    print('finish! time = ', str(tm.strftime("%Y-%m-%d %H:%M:%S", tm.localtime())))
    return


if __name__ == '__main__':

    for seed in [10]:
        all_data_path = '../dataset/2db/50/2db_seed{0}_after1970.csv'.format(seed)
        test_data_path_after1970 = '../dataset/2db/50/2db_seed{0}_after1970.csv'.format(seed)
        test_data_path_before1970 = '../dataset/2db/50/2db_seed{0}_before1970.csv'.format(seed)
        test_data_path_other = '../dataset/2db/50/2db_seed{0}_other.csv'.format(seed)
        file_name = 'validation_seed{0}'.format(seed)

        train_X, train_Y, valid_X, valid_Y, x_min, x_max, y_min, y_max, col_data_x = read_split_data(all_data_path)
        test_X_after1970, test_Y_after1970, x_min, x_max, y_min, y_max, col_x = read_data(test_data_path_after1970)
        test_X_before1970, test_Y_before1970, x_min, x_max, y_min, y_max, col_x = read_data(test_data_path_before1970)
        test_X_other, test_Y_other, x_min, x_max, y_min, y_max, col_x = read_data(test_data_path_other)

        train(seed, all_data_path, file_name)
