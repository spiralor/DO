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

    DstFilePath = r'soda3.4.2_mn_ocean_reg_{0}.nc'.format(str(yearIndex))
    DstFilePath2 = result_path + '{0}_do_predict_zip.nc'.format(str(yearIndex))
    
    X = Dataset(to_predict_path + DstFilePath)
    # 检索定位印度洋的lon-lat box
    lon_min = 21.75
    lon_max = 151.25
    lat_min = -49.75
    lat_max = 26.75
    lon = X.variables['xt_ocean'][:]
    lat = X.variables['yt_ocean'][:]
    lon_min_index = np.where(lon == lon_min)[0][0]
    lon_max_index = np.where(lon == lon_max)[0][0]
    lat_min_index = np.where(lat == lat_min)[0][0]
    lat_max_index = np.where(lat == lat_max)[0][0]

    etop_nc = Dataset('etop_soda342.nc')
    etop = etop_nc.variables['etop'][lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    time_units = X.variables['time'].units
    time = X.variables['time'][:]
    year = yearIndex
    depth = X.variables['st_ocean'][:]
    lat = X.variables['yt_ocean'][lat_min_index:lat_max_index + 1]
    lon = X.variables['xt_ocean'][lon_min_index:lon_max_index + 1]
    taux = X.variables['taux'][:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    tauy = X.variables['tauy'][:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    ssh = X.variables['ssh'][:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    u = X.variables['u'][:,:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    v = X.variables['v'][:,:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    w = X.variables['wt'][:,:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    salt = X.variables['salt'][:,:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    temp = X.variables['temp'][:,:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    mlp = X.variables['mlp'][:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    mls = X.variables['mls'][:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    mlt = X.variables['mlt'][:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    net_heating = X.variables['net_heating'][:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    prho = X.variables['prho'][:,:,lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    
    X.close()
    etop_nc.close()

    Y = Dataset(DstFilePath2, 'w', format='NETCDF4')
    Y.createDimension('time', time.shape[0])
    Y.createDimension('depth', depth.shape[0])
    Y.createDimension('lat', lat.shape[0])
    Y.createDimension('lon', lon.shape[0])
    Y.createDimension('bnds', 2)
    
    Y.createVariable('o2_pred', 'f', ('time', 'depth', 'lat', 'lon'), fill_value=-9.99E33, zlib=True)
    Y.createVariable('depth_bnds', 'f', ('depth', 'bnds'), fill_value=-9.99E33, zlib=True)
    Y.createVariable('time', 'd', ('time'), zlib=True)
    Y.createVariable('depth', 'd', ('depth'), zlib=True)
    Y.createVariable('lat', 'd', ('lat'), zlib=True)
    Y.createVariable('lon', 'd', ('lon'), zlib=True)
    Y.variables['time'][:] = time
    Y.variables['depth'][:] = depth
    Y.variables['depth'].bounds = "depth_bnds"
    Y.variables['lat'][:] = lat
    Y.variables['lon'][:] = lon
    
    Z = Dataset('soda342_depth_height_bnds.nc')
    bnds = Z.variables['st_ocean_bnds'][:]
    Z.close()
    Y.variables['depth_bnds'][:] = bnds
    Y.variables['time'].units = time_units
    Y.variables['lat'].units = "degrees_N"
    Y.variables['lon'].units = "degrees_E"
    

    region_path = 'sea_num_soda342.nc'
    sea_region_nc = Dataset(region_path)

    sea_region = sea_region_nc.variables['sea_num'][lat_min_index:lat_max_index + 1, lon_min_index:lon_max_index + 1]
    sea_region_ravel = sea_region.ravel()
    sea_region_list = (sea_region_ravel != 14) & (sea_region_ravel != 15) & (sea_region_ravel != 16) & (sea_region_ravel != 17) & (sea_region_ravel != 18) & (sea_region_ravel != 19)
    sea_region_nc.close()

    for monthIndex in range(0, 12):
    #for monthIndex in [3-1, 6-1, 9-1]:
        for depthIndex in range(0, 50):
            month = monthIndex + 1
            _lon = np.tile(lon, lat.shape[0]).reshape(-1, 1).ravel()
            _lat = np.repeat(lat, lon.shape[0]).ravel()
            _year = np.repeat(year, lat.shape[0] * lon.shape[0]).ravel()
            _month = np.repeat(month, lat.shape[0] * lon.shape[0]).ravel()
            _depth = np.repeat(depth[depthIndex], lat.shape[0] * lon.shape[0]).ravel()                 
            _u = u[monthIndex, depthIndex, :].ravel()
            _v = v[monthIndex, depthIndex, :].ravel()
            _w = w[monthIndex, depthIndex, :].ravel()
            _temp = temp[monthIndex, depthIndex, :].ravel()
            _salt = salt[monthIndex, depthIndex, :].ravel()
            _ssh = ssh[monthIndex, :].ravel()
            _taux = taux[monthIndex, :].ravel()
            _tauy = tauy[monthIndex, :].ravel()
            _etop = etop.ravel()
            _mlp = mlp[monthIndex, :].ravel()
            _mls = mls[monthIndex, :].ravel()
            _mlt = mlt[monthIndex, :].ravel()
            _net_heating = net_heating[monthIndex, :].ravel()
            _prho = prho[monthIndex, depthIndex, :].ravel()
     
            X_pred = np.c_[_lon, _lat, _year, _month, _depth, _u, _v, _w, _temp, _salt, _ssh, _taux, _tauy, _etop, 
                           _mlp, _mls, _mlt, _net_heating, _prho]            
            #X_mask_data = np.c_[_u, _v, _w, _temp, _salt, _ssh, _taux, _tauy, _mlp, _mls, _mlt, _net_heating, _prho]
            X_mask_data = np.c_[_temp,_salt]
            mask_all = np.matrix(X_mask_data < -100000000)
            mask_all = np.c_[mask_all, sea_region_list]
            #mask_mul = np.matrix([True, True, True, True, True, True, True, True, True, True, True, True, True, True]).T
            mask_mul = np.matrix([True, True]).T
            mask = np.matmul(np.array(mask_all), mask_mul)
            
            judge_index = np.where(mask == False)
            if len(judge_index[0]) == 0:
                continue

            y_pred = np.zeros(len(mask))
            y_pred[judge_index[0]] = model.predict(X_pred[judge_index[0],:])
            y_pred[y_pred < 0] = 0
            y_pred = ma.masked_array(y_pred, mask=mask)
            y_pred = y_pred.reshape(lat.shape[0], lon.shape[0])
            Y.variables['o2_pred'][monthIndex, depthIndex, :, :] = y_pred
    Y.close()
    print(str(yearIndex) + ' end! ' + str(tm.strftime("%Y-%m-%d %H:%M:%S", tm.localtime())))

def cal_OMZ(index):

    file_name = '{0}_do_predict_zip.nc'.format(index)
    # OMZ 20
    range_shell = 'cdo -z zip -lec,20 -vertmin -timmin {1}/{0}_do_predict_zip.nc {2}/{0}_omz_select_region.nc'.format(index, model_path,dst_path_20)
    os.system(range_shell)
    area_shell = 'cdo -z zip -fldsum -mul {1}/{0}_omz_select_region.nc {3}/gr_area_soda342_Indian.nc {2}/{0}_omz_area.nc'.format(index, dst_path_20, dst_path_20_area, tool_path)
    os.system(area_shell)
    
    return 

if __name__ == '__main__':
    

    model_list = ['et_200_None_2_fit_2db_Indian_train_remask.pkl']
    #model_list = ['et_200_15_2_fit_2db_Indian_train_index0.pkl','et_200_20_2_fit_2db_Indian_train_index0.pkl','et_200_30_2_fit_2db_Indian_train_index0.pkl','et_200_40_2_fit_2db_Indian_train_index0.pkl']
                
    for model_item in model_list:
        model_pkl = model_item
        model_name = os.path.splitext(model_pkl)[0]
        model_path = r'../../model/' + model_pkl
        to_predict_path = r'/home/zju/do4/soda342/'
        result_path = r'../' + model_name + '/'
        if not os.path.exists(result_path):
            os.makedirs(result_path)

        model = joblib.load(model_path)
        model.set_params(verbose=0,n_jobs=1)
        print('load model ' + str(model_path))
        
        items = [ i for i in range(1980,2020)]
        tup = np.c_[items, np.repeat(result_path, len(items))]
        pool = Pool(40)
        pool.map(model_predict, tup)
        pool.close()
        pool.join()
       

        cdoshell = 'bash ./o2_sp_test_full_soda342.sh {0}'.format(model_name)
        val = os.system(cdoshell) 
        
        # 计算OMZ
        model_path = '../' + model_name
        dst_path_20 =  '../cdo_{0}/OMZ_20'.format(model_name)
        tool_path = '/home/zju/do5/predict/code'
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
            shell ='cdo -setctomiss,0 -expr,"year_label = o2_pred*{0}"  {1}/{2}  {1}/{0}_omz_select_region_year_label.nc'.format(year_index,dst_path_20,file_name)
            os.system(shell)
            
        shell = 'cdo -ensmin '
        for year_index in year:
            file_name = ' {1}/{0}_omz_select_region_year_label.nc '.format(year_index,dst_path_20)
            shell += file_name
        shell += ' {0}/OMZ_start_yr.nc'.format(dst_path_20)
        print(shell)
        
        os.system(shell)
        
        delete_shell = '\ncp -r ../cdo_{0} /home/zju/do4/do_predict/predict/Indian_ANALYZE/' \
                '\ncp -r ../{0} /home/zju/do4/do_predict/predict/Indian_RESULT/' \
                '\nrm -rf ../{0}' \
                '\nrm -rf ../cdo_{0}' \
                '\necho \"finish\"'.format(model_name)        
        val = os.system(delete_shell)           
        print( model_name + ' end! ' + str(tm.strftime("%Y-%m-%d %H:%M:%S", tm.localtime())))
