import os
import numpy as np

soda_path = '/home/zju/do5/soda342'
year_np = np.arange(1980,2020,1)

#for year in year_np:
    #cdo_shell = 'cdo -z zip -expr,"o2P = o2_pred/1e6/1e15*1027"  -fldsum -mul ../et_200_leaf20_fit_2db_Indian_train_2023/{0}_do_predict_zip.nc Indian_full_area.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_1027_depth/total/{0}_o2_storage_d.nc'.format(year)
    #os.system(cdo_shell)

#for year in year_np:
    #cdo_shell = 'cdo -z zip -expr,"o2P = o2_pred/1e6/1e15" -mul -fldsum -mul -mul ../et_200_leaf20_fit_2db_Indian_train_2023/{0}_do_predict_zip.nc -sellonlatbox,21.75,151.25,-49.75,26.75 -selname,prho ../../soda342/soda3.4.2_mn_ocean_reg_{0}.nc Indian_full_area.nc soda342_depth_height.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/total/{0}_o2_storage_d.nc'.format(year)
    #os.system(cdo_shell)


#cdo_shell = 'cdo -z zip -mergetime ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/total/*.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_d_total.nc'
#os.system(cdo_shell)

#cdo_shell = 'cdo -z zip -vertsum ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_d_total.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_total.nc'
#os.system(cdo_shell)

#cdo_shell = 'cdo -z zip -yearmean ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_total.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_total_yearmean.nc'
#os.system(cdo_shell)

#cdo_shell = 'cdo -z zip -yearstd ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_total.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_total_yearstd.nc'
#os.system(cdo_shell)

for region in ['AB','BB','SIO','EIO']:

    cdo_shell = 'cdo -z zip -mergetime ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_1027_depth/{0}/*.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_1027_depth/o2_storage_d_{0}.nc'.format(region)
    os.system(cdo_shell)

    cdo_shell = 'cdo -z zip -yearmean ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_1027_depth/o2_storage_d_{0}.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_1027_depth/o2_storage_d_{0}_yearmean.nc'.format(region)
    os.system(cdo_shell)

    cdo_shell = 'cdo -z zip -trend ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_1027_depth/o2_storage_d_{0}_yearmean.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_1027_depth/o2_storage_d_{0}_yearmean_trend_a.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_1027_depth/o2_storage_d_{0}_yearmean_trend_b.nc'.format(region) 
    os.system(cdo_shell)

    #for year in year_np:
        #cdo_shell = 'cdo -z zip -expr,"o2P = o2_pred/1e6/1e15*1027" -fldsum -mul ../et_200_leaf20_fit_2db_Indian_train_2023/{0}_do_predict_zip.nc {1}_area.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_1027_depth/{1}/{0}_o2_storage_d.nc'.format(year, region)
        #os.system(cdo_shell)

        #cdo_shell = 'cdo -z zip -expr,"o2P = o2_pred/1e6/1e15"  -fldsum -mul -mul ../et_200_leaf20_fit_2db_Indian_train_2023/{0}_do_predict_zip.nc -sellonlatbox,21.75,151.25,-49.75,26.75 -selname,prho ../../soda342/soda3.4.2_mn_ocean_reg_{0}.nc {1}_area.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/{1}/{0}_o2_storage_d.nc'.format(year, region)
        #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -mergetime ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/{0}/*.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_d_{0}.nc'.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -vertsum ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_d_{0}.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}.nc'.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -yearmean ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_yearmean.nc'.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -yearstd ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_yearstd.nc'.format(region)
    #os.system(cdo_shell)

#for region in ['AB','BB','SIO','EIO']:
    #cdo_shell = 'cdo -z zip -vertsum -sellevidx,1/17 ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_d_{0}.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_0-200.nc'.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -vertsum -sellevidx,18/24 ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_d_{0}.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_200-500.nc'.format(region)
    #os.system(cdo_shell)


    #cdo_shell = 'cdo -z zip -vertsum -sellevidx,25/28 ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_d_{0}.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_500-1000.nc'.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -vertsum -sellevidx,29/34 ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_d_{0}.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_1000-2000.nc'.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -vertsum -sellevidx,35/39 ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_d_{0}.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_2000-3000.nc'.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -vertsum -sellevidx,40/43 ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_d_{0}.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_3000-4000.nc'.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -vertsum -sellevidx,44/50 ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_d_{0}.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_4000-5000.nc'.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -yearmean ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_0-200.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_0-200_yearmean.nc'.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -yearmean ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_200-500.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_200-500_yearmean.nc'.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -yearmean ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_500-1000.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_500-1000_yearmean.nc'.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -yearmean ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_1000-2000.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_1000-2000_yearmean.nc'.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -yearmean ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_2000-3000.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_2000-3000_yearmean.nc'.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -yearmean ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_3000-4000.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_3000-4000_yearmean.nc'.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -yearmean ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_4000-5000.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_storage_depth/o2_storage_{0}_4000-5000_yearmean.nc'.format(region)
    #os.system(cdo_shell)

#cdo_shell = 'cdo -z zip -fldmean ../et_200_leaf20_fit_2db_Indian_train_2023/do_predict.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_concentration_depth/total/o2_c_fldmean.nc'
#os.system(cdo_shell)

#for region in ['AB','BB','SIO','EIO']:

    #cdo_shell = 'cdo -z zip -fldmean -mul ../et_200_leaf20_fit_2db_Indian_train_2023/do_predict.nc {0}_area_mask.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_concentration_depth/{0}/o2_c_fldmean.nc'.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -vertmean -sellevidx,1/17 -fldmean -mul ../et_200_leaf20_fit_2db_Indian_train_2023/do_predict.nc {0}_area_mask.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_concentration_depth/{0}/o2_c_fldmean_vertmean_0_200.nc '.format(region)
    #os.system(cdo_shell)
    
    #cdo_shell = 'cdo -z zip -vertmean -sellevidx,18/24 -fldmean -mul ../et_200_leaf20_fit_2db_Indian_train_2023/do_predict.nc {0}_area_mask.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_concentration_depth/{0}/o2_c_fldmean_vertmean_200_500.nc '.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -vertmean -sellevidx,25/28 -fldmean -mul ../et_200_leaf20_fit_2db_Indian_train_2023/do_predict.nc {0}_area_mask.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_concentration_depth/{0}/o2_c_fldmean_vertmean_500_1000.nc '.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -vertmean -sellevidx,29/34 -fldmean -mul ../et_200_leaf20_fit_2db_Indian_train_2023/do_predict.nc {0}_area_mask.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_concentration_depth/{0}/o2_c_fldmean_vertmean_1000_2000.nc '.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -vertmean -sellevidx,35/39 -fldmean -mul ../et_200_leaf20_fit_2db_Indian_train_2023/do_predict.nc {0}_area_mask.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_concentration_depth/{0}/o2_c_fldmean_vertmean_2000_3000.nc '.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -vertmean -sellevidx,40/43 -fldmean -mul ../et_200_leaf20_fit_2db_Indian_train_2023/do_predict.nc {0}_area_mask.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_concentration_depth/{0}/o2_c_fldmean_vertmean_3000_4000.nc '.format(region)
    #os.system(cdo_shell)

    #cdo_shell = 'cdo -z zip -vertmean -sellevidx,44/50 -fldmean -mul ../et_200_leaf20_fit_2db_Indian_train_2023/do_predict.nc {0}_area_mask.nc ../cdo_et_200_leaf20_fit_2db_Indian_train_2023/o2_concentration_depth/{0}/o2_c_fldmean_vertmean_4000_5000.nc '.format(region)
    #os.system(cdo_shell)


