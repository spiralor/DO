from netCDF4 import Dataset, num2date
import numpy as np
import numpy.ma as ma
import joblib
import time as tm
import os
import gc
import threading
from multiprocessing import Pool

def model_predict(tup):
    yearIndex = tup[0]
    result_path = tup[1]
    print(str(yearIndex) + ' start! ' + str(tm.strftime("%Y-%m-%d %H:%M:%S", tm.localtime())))

    DstFilePath = r'{0}_do_predict_zip.nc'.format(str(yearIndex))
    DstFilePath2 = result_path + DstFilePath

    X = Dataset(to_predict_path + DstFilePath)
    time_units = X.variables['time'].units
    time = X.variables['time'][:]
    real_time = num2date(time, time_units, '360_day')
    year = real_time[0].year
    depth = X.variables['depth'][:]
    lat_new = X.variables['lat'][:]
    lon_new = X.variables['lon'][:]
    lon_new2 =  np.r_[X.variables['lon'][0:360], X.variables['lon'][360:720] - 360]
    taux = X.variables['taux'][:]
    tauy = X.variables['tauy'][:]
    ssh = X.variables['ssh'][:]
    u = X.variables['u'][:]
    v = X.variables['v'][:]
    w = X.variables['w'][:]
    salt = X.variables['salt'][:]
    temp = X.variables['temp'][:]
    cmip6_o2 = X.variables['cmip6_cdo_o2'][:]
    etop = X.variables['etop'][:]
    X.close()

    Y = Dataset(DstFilePath2, 'w', format='NETCDF4')
    Y.createDimension('time', 12)
    Y.createDimension('depth', 40)
    Y.createDimension('lat', 330)
    Y.createDimension('lon', 720)
    Y.createDimension('bnds', 2)
    if str(Y.variables.keys()).find('o2_pred_rf') == -1:
        Y.createVariable('o2_pred_rf', 'f', ('time', 'depth', 'lat', 'lon'), fill_value=-9.99E33, zlib=True)
    if str(Y.variables.keys()).find('depth_bnds') == -1:
        Y.createVariable('depth_bnds', 'f', ('depth', 'bnds'), fill_value=-9.99E33, zlib=True)
    Y.createVariable('time', 'd', ('time'), zlib=True)
    Y.createVariable('depth', 'd', ('depth'), zlib=True)
    Y.createVariable('lat', 'd', ('lat'), zlib=True)
    Y.createVariable('lon', 'd', ('lon'), zlib=True)
    Y.variables['time'][:] = time
    Y.variables['depth'][:] = depth
    Y.variables['depth'].bounds = "depth_bnds"
    Y.variables['lat'][:] = lat_new
    Y.variables['lon'][:] = lon_new
    
    Z = Dataset('1871_do_predict_zip.nc')
    bnds = Z.variables['depth_bnds'][:]
    Z.close()
    Y.variables['depth_bnds'][:] = bnds
    Y.variables['time'].units = time_units
    Y.variables['lat'].units = "degrees_north"
    Y.variables['lon'].units = "degrees_east"

    for monthIndex in [3-1,6-1,9-1]:
    #for monthIndex in range(0, 12):
        for depthIndex in range(0, 40):
            month1 = real_time.data[monthIndex].month
            _lon2 = np.tile(lon_new2, 330).reshape(-1, 1).ravel()
            _lat = np.repeat(lat_new, 720).ravel()
            _year = np.repeat(year, 330 * 720).ravel()
            _month1 = np.repeat(month1, 330 * 720).ravel()
            _depth = np.repeat(depth[depthIndex], 330 * 720).ravel()
            _u = u[monthIndex, depthIndex, :].ravel()
            _v = v[monthIndex, depthIndex, :].ravel()
            _w = w[monthIndex, depthIndex, :].ravel()
            _temp = temp[monthIndex, depthIndex, :].ravel()
            _salt = salt[monthIndex, depthIndex, :].ravel()
            _ssh = ssh[monthIndex, :].ravel()
            _taux = taux[monthIndex, :].ravel()
            _tauy = tauy[monthIndex, :].ravel()
            _etop = etop.ravel()
            _cmip6 = cmip6_o2[monthIndex, depthIndex, :].ravel()
            _cmip6[np.isnan(_cmip6)] = -9.98999971e+33
            X_pred = np.c_[_lon2, _lat, _year, _month1, _depth, _u, _v, _w, _temp, _salt, _ssh, _taux, _tauy, _etop, _cmip6]
            X_mask_data = np.c_[_u, _v, _w, _temp, _salt, _ssh, _taux, _tauy, _cmip6]

            mask_all = np.matrix(X_mask_data < -9e+33)
            mask_mul = np.matrix([True, True, True, True, True, True, True, True, True]).T
            mask = np.matmul(mask_all, mask_mul)

            judge_index = np.where(mask == False)
            y_pred = np.zeros(len(mask))
            y_pred[judge_index[0]] = model.predict(X_pred[judge_index[0],:])
            y_pred[y_pred < 0] = 0
            y_pred = ma.masked_array(y_pred, mask=mask)
            y_pred = y_pred.reshape(330, 720)
            Y.variables['o2_pred_rf'][monthIndex, depthIndex, :, :] = y_pred
    Y.close()
    print(str(yearIndex) + ' end! ' + str(tm.strftime("%Y-%m-%d %H:%M:%S", tm.localtime())))

def cal_OMZ(index):

    file_name = '{0}_do_predict_zip.nc'.format(index)
    # OMZ 20
    range_shell = 'cdo -z zip -lec,20 -vertmin -timmean {1}/{0}_do_predict_zip.nc {2}/{0}_omz_select_region.nc'.format(index, model_path,dst_path_20)
    os.system(range_shell)
    area_shell = 'cdo -z zip -fldsum -mul {1}/{0}_omz_select_region.nc {3}/gr_area.nc {2}/{0}_omz_area.nc'.format(index, dst_path_20, dst_path_20_area, tool_path)
    os.system(area_shell)
    
    return 

if __name__ == '__main__':
    
    model_list = ['et_400_20_fit_2db_f4_del1986_seed1_after1970_-180-180lon_lt20_sw100.pkl']
                          
    for model_item in model_list:

        model_pkl = model_item
        model_name = os.path.splitext(model_pkl)[0]
        RF_model_path = r'../../model/' + model_pkl
        to_predict_path = r'../nc_predict_zip_dataset/'
        result_path = r'../' + model_name + '/'
        if not os.path.exists(result_path):
            os.makedirs(result_path)

        model = joblib.load(RF_model_path)
        model.set_params(verbose=0,n_jobs=-1)
        print('load model ' + str(RF_model_path))
        
        items = [ i for i in range(1871,2011)]
        tup = np.c_[items, np.repeat(result_path, len(items))]
        pool = Pool(90)
        pool.map(model_predict, tup)
        pool.close()
        pool.join()
       
        cdoshell = 'bash ./o2_sp_test.sh {0}'.format(model_name)
        val = os.system(cdoshell) 
        
        # 计算OMZ
        model_path = '../' + model_name
        dst_path_20 =  '../cdo_{0}/OMZ_20'.format(model_name)
        tool_path = '/home/zju/do4/do_predict/predict/'
        if not os.path.exists(dst_path_20):
            os.makedirs(dst_path_20)            
        dst_path_20_area = '../cdo_{0}/OMZ_20/OMZ_20_area'.format(model_name)
        if not os.path.exists(dst_path_20_area):
            os.makedirs(dst_path_20_area)   

        year = items
        pool = Pool()
        pool.map(cal_OMZ, year)
        pool.close()
        pool.join()            
            
        # 合并OMZ_20_area
        shell_20 = 'cdo mergetime {0}/*.nc {0}/OMZ_area_timeseries.nc'.format(dst_path_20_area)
        os.system(shell_20)
        
        # 统计OMZ_20出现频次
        shell = 'cdo enssum '
        for year_index in year:
            shell += ' {0}/{1}_omz_select_region.nc '.format(dst_path_20,year_index)
        shell += ' {0}/OMZ_20_freqcy.nc'.format(dst_path_20)
        os.system(shell)
        
        # 统计最早OMZ_20出现时间    
        for year_index in year:
            file_name = '{0}_omz_select_region.nc'.format(year_index)
            shell ='cdo -setctomiss,0 -expr,"year_label = o2_pred_rf*{0}"  {1}/{2}  {1}/{0}_omz_select_region_year_label.nc'.format(year_index,dst_path_20,file_name)
            os.system(shell)
            
        shell = 'cdo -ensmin '
        for year_index in year:
            file_name = ' {1}/{0}_omz_select_region_year_label.nc '.format(year_index,dst_path_20)
            shell += file_name
        shell += ' {0}/OMZ_start_yr.nc'.format(dst_path_20)
        print(shell)
        
        os.system(shell)
        
        delete_shell = '\ncp -r ../cdo_{0} /home/zju/do4/do_predict/predict/50_ANALYZE/' \
                '\ncp -r ../{0} /home/zju/do4/do_predict/predict/50_RESULT/' \
                '\nrm -rf ../{0}' \
                '\nrm -rf ../cdo_{0}' \
                '\necho \"finish\"'.format(model_name)        
        val = os.system(delete_shell)           
        print( model_name + ' end! ' + str(tm.strftime("%Y-%m-%d %H:%M:%S", tm.localtime())))
