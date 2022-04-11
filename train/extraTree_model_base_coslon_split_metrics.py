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

seed = 7

def read_data(all_data_path):
    all_data = pd.read_csv(all_data_path, engine='python')
    lon_col = 'Longitude'
    lat_col = 'Latitude'
    depth = 'depth(m)'
    year_col = 'Year'
    month_col = 'Month'                
    col_data_x = [lon_col, lat_col, year_col, month_col, depth, 'u', 'v', 'w', 'temp', 'salt', 'ssh', 'taux', 'tauy', 'ETOPO1_Bed', 'o2_CMIP6']
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
     
    all_data_x[:,0] = np.cos(np.pi/180 * all_data_x[:,0])
    train_data_x = all_data_x
    train_data_y = all_data_y

    return train_data_x, train_data_y, x_min, x_max, y_min, y_max, col_data_x

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
    
def test_stats(X, Y, flag, model):
    y_pred = model.predict(X).squeeze()
    y_real = Y
    count = len(y_real)
    stats = cal_statistic(y_real, y_pred)
    out_str = '[ET model] %s Count:%d, R2:%.8f, RMSE:%.3f, MAE:%.3f, MAPE:%.3f, r:%.3f.' % tuple([flag] + [count] + stats)

    return out_str, y_pred

def train(seed, file_name, work_log):
    f_worker = open(work_log, "w")

    model_estimate=ExtraTreesRegressor(random_state=seed, n_jobs=-1,n_estimators=200,max_depth=None,min_samples_split=2)
    model_estimate.fit(train_X, train_Y)
    
    f_worker.write('Feature Importances:\n')
    feat_import_list = [(col_x[i], model_estimate.feature_importances_[i]) for i in range(len(col_x))]
    feat_import_list_sorted = sorted(feat_import_list, key=lambda x: x[1], reverse=True)
    for i in feat_import_list_sorted:
        f_worker.write(str(i[0])+ ':'+ str(i[1])+'\n')
    n_features = model_estimate.n_features_
    f_worker.write('n_Feature:' + str(n_features)+'\n')

    f_worker.write('Prediciton Accuracy After1970:'+'\n')
    train_results_after1970,y_pred = test_stats(test_X_after1970, test_Y_after1970, 'After1970', model_estimate)
    f_worker.write(train_results_after1970 + '\n')
    
    f_worker.write('Predicion Accuracy Before1970:'+'\n')
    test_results_before1970, y_pred = test_stats(test_X_before1970, test_Y_before1970, 'Before1970', model_estimate)
    f_worker.write(test_results_before1970 + '\n')
    
    f_worker.write('Predicion Accuracy Balance:'+'\n')
    test_results_balance, y_pred = test_stats(test_X_balance, test_Y_balance, 'Balance', model_estimate)
    f_worker.write(test_results_balance + '\n')

    f_worker.write('Predicion Accuracy Other:'+'\n')
    test_results_other, y_pred = test_stats(test_X_other, test_Y_other, 'Other', model_estimate)
    f_worker.write(test_results_other + '\n')
    
    joblib.dump(model_estimate, '/home/zju/do5/model/' + file_name + '.pkl')
    f_worker.close()
    print('finish!')
    
if __name__ == '__main__':
    
    db = ['2db_v0_i0']
    for db_item in db:

        test_data_path_after1970  = '../dataset/2db/{0}/GRIDDING_{0}_b0_seed1_after1960.csv'.format(db_item)
        test_data_path_before1970 = '../dataset/2db/{0}/GRIDDING_{0}_b0_seed1_before1970.csv'.format(db_item)
        test_data_path_other = '../dataset/2db/{0}/GRIDDING_{0}_b0_seed1_other.csv'.format(db_item)
        test_data_path_balance = '../dataset/2db/{0}/GRIDDING_{0}_b0_seed1.csv'.format(db_item)
        
        test_X_after1970, test_Y_after1970, x_min, x_max, y_min, y_max, col_x = read_data(test_data_path_after1970)
        test_X_before1970, test_Y_before1970, x_min, x_max, y_min, y_max, col_x = read_data(test_data_path_before1970)
        test_X_other, test_Y_other, x_min, x_max, y_min, y_max, col_x = read_data(test_data_path_other)
        test_X_balance, test_Y_balance, x_min, x_max, y_min, y_max, col_x = read_data(test_data_path_balance)

        #train_X = np.r_[test_X_balance, test_X_other]
        #train_Y = np.r_[test_Y_balance, test_Y_other]
        train_X = test_X_after1970
        train_Y = test_Y_after1970

        train_data_name = '{0}_b0_seed1_after1960'.format(db_item)
        file_name = 'et_200_none_2_fit_{0}_cos_lon'.format(train_data_name)
        work_log =  file_name + '.log'
        train(seed, file_name, work_log)
