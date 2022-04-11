oceanlist=('Antarctic' 'Arctic' 'Atlantic' 'Indian' 'Pacific')
targetPath='../cdo_'$*''
mkdir -p ${targetPath}/o2_spatial_depth/
mkdir -p ${targetPath}/o2_storage_depth/
mkdir -p ${targetPath}/o2_vertmin/
for file in ../$*/*
do
	{
		file_name=${file##*/}
		file_year=${file_name%%_*}
		cdo -z zip -mul -expr,'o2s = o2_pred_rf/1000/0.9737' -timmean ${file} full_depth_height.nc ${targetPath}/o2_spatial_depth/${file_year}_o2_spatial_d.nc
		cdo -mul -fldsum -expr,'o2P = o2_pred_rf/1000000000000000000/0.9737' -mul ${file} gr_area.nc depth_height.nc ${targetPath}/o2_storage_depth/${file_year}_o2_Pmol.nc
		cdo -z zip -vertmin -timmin ${file} ${targetPath}/o2_vertmin/${file_year}_vertmin.nc
		echo ${file_year}
		
		
	}&
done
wait
#逐层变化量 mol m-2
cdo -z zip -mergetime ${targetPath}/o2_spatial_depth/*.nc ${targetPath}/o2_spatial_d.nc
#逐层储量(月平均) Pmol
cdo -mergetime ${targetPath}/o2_storage_depth/*.nc ${targetPath}/o2P_d_m.nc
#水柱最低浓度
cdo -z zip -mergetime ${targetPath}/o2_vertmin/*.nc ${targetPath}/o2_spatial_vertmin.nc &
#总空间变化量 mol m-2
cdo -z zip -vertsum ${targetPath}/o2_spatial_d.nc ${targetPath}/o2_spatial.nc
#1000以下空间变化量 mol m-2
cdo -z zip -vertsum -sellevidx,23/40 ${targetPath}/o2_spatial_d.nc ${targetPath}/o2_spatial_1000_5000.nc
#1871-2010总趋势 mol m-2 year
cdo -z zip -trend ${targetPath}/o2_spatial.nc ${targetPath}/o2_spatial_trend_a.nc ${targetPath}/o2_spatial_trend_b.nc &
#1871-2010 1000以下趋势 mol m-2 year
cdo -z zip -trend ${targetPath}/o2_spatial_1000_5000.nc ${targetPath}/o2_spatial_1000_5000_trend_a.nc ${targetPath}/o2_spatial_1000_5000_trend_b.nc &
#1980-2010趋势 mol m-2 year
cdo -z zip -trend -selyear,1980/2010 ${targetPath}/o2_spatial.nc ${targetPath}/o2_spatial_trend_1980_a.nc ${targetPath}/o2_spatial_trend_1980_b.nc &
#1980-2010 1000以下趋势 mol m-2 year
cdo -z zip -trend -selyear,1980/2010 ${targetPath}/o2_spatial_1000_5000.nc ${targetPath}/o2_spatial_1000_5000_trend_1980_a.nc ${targetPath}/o2_spatial_1000_5000_trend_1980_b.nc &


#总空间变化量140年平均 mol m-2
cdo -z zip -timmean ${targetPath}/o2_spatial.nc ${targetPath}/o2_spatial_timmean.nc
#总空间变化量距平 mol m-2
cdo -z zip -sub ${targetPath}/o2_spatial.nc ${targetPath}/o2_spatial_timmean.nc ${targetPath}/o2_spatial-timmean.nc &

#逐层储量 Pmol
cdo -yearmean ${targetPath}/o2P_d_m.nc ${targetPath}/o2P_d.nc &
#总储量 Pmol
cdo -expr,'o2P = o2s/1000000000000000' -fldsum -mul ${targetPath}/o2_spatial.nc gr_area.nc ${targetPath}/o2P.nc &
#>1000米储量 Pmol
cdo -expr,'o2P = o2s/1000000000000000' -fldsum -mul ${targetPath}/o2_spatial_1000_5000.nc gr_area.nc ${targetPath}/o2P_1000_5000.nc &

#各海区储量 Pmol
for oceanname in ${oceanlist[@]}
do	{
		#总储量
		cdo -expr,'o2P = o2s/1000000000000000' -fldsum  -mul ${targetPath}/o2_spatial.nc ${oceanname}_area.nc ${targetPath}/o2P_${oceanname}.nc
		#>1000米
		cdo -expr,'o2P = o2s/1000000000000000' -fldsum  -mul ${targetPath}/o2_spatial_1000_5000.nc ${oceanname}_area.nc ${targetPath}/o2P_${oceanname}_1000_5000.nc
	}&
done
wait

mkdir -p ${targetPath}/o2c_μmol_kg-1_spatial_1200_5000/

for file in ../$*/*
do
        {
                file_name=${file##*/}
                file_year=${file_name%%_*}
                #cdo -z zip -expr,'o2c = o2_pred_rf * 1.027' -intlevel,100 -timmean ${file} ${targetPath}/o2c_mmol_m-3_spatial_100/${file_year}_o2c_spatial_100.nc
                #cdo -z zip -expr,'o2c = o2_pred_rf * 1.027' -intlevel,400 -timmean ${file} ${targetPath}/o2c_mmol_m-3_spatial_400/${file_year}_o2c_spatial_400.nc
                #cdo -z zip -expr,'o2c = o2_pred_rf * 1.027' -intlevel,700 -timmean ${file} ${targetPath}/o2c_mmol_m-3_spatial_700/${file_year}_o2c_spatial_700.nc
                #cdo -z zip -expr,'o2c = o2_pred_rf * 1' -vertmean -sellevidx,1/23 -timmean ${file} ${targetPath}/o2c_μmol_kg-1_spatial_0_1200/${file_year}_o2c_spatial_0_1200.nc
                cdo -z zip -expr,'o2c = o2_pred_rf * 1' -vertmean -sellevidx,23/40 -timmean ${file} ${targetPath}/o2c_μmol_kg-1_spatial_1200_5000/${file_year}_o2c_spatial_1200_5000.nc
                # cdo -z zip -vertmin -expr,'o2c = o2_pred_rf *1/0.9737*1.4/44.4' -timmin ${file} ${targetPath}/o2_vertmin_mg_L-1/${file_year}_vertmin_mg_L-1.nc
                echo ${file_year}
        }&
done
wait

#100 400 700变化时间合并 mmol m-3
#cdo -z zip -mergetime ${targetPath}/o2c_mmol_m-3_spatial_100/*.nc ${targetPath}/o2c_mmol_m-3_spatial_100.nc
#cdo -z zip -mergetime ${targetPath}/o2c_mmol_m-3_spatial_400/*.nc ${targetPath}/o2c_mmol_m-3_spatial_400.nc
#cdo -z zip -mergetime ${targetPath}/o2c_mmol_m-3_spatial_700/*.nc ${targetPath}/o2c_mmol_m-3_spatial_700.nc
#0-1200 1200-5000变化时间合并 μmol kg-1
#cdo -z zip -mergetime ${targetPath}/o2c_μmol_kg-1_spatial_0_1200/*.nc ${targetPath}/o2c_μmol_kg-1_spatial_0_1200.nc
cdo -z zip -mergetime ${targetPath}/o2c_μmol_kg-1_spatial_1200_5000/*.nc ${targetPath}/o2c_μmol_kg-1_spatial_1200_5000.nc
#1958-2010 trend mmol m-3 year-1
#cdo -z zip -trend -selyear,1958/2010 ${targetPath}/o2c_mmol_m-3_spatial_100.nc ${targetPath}/o2c_mmol_m-3_spatial_100_trend_1958_a.nc ${targetPath}/o2c_mmol_m-3_spatial_100_trend_1958_b.nc &
#cdo -z zip -trend -selyear,1958/2010 ${targetPath}/o2c_mmol_m-3_spatial_400.nc ${targetPath}/o2c_mmol_m-3_spatial_400_trend_1958_a.nc ${targetPath}/o2c_mmol_m-3_spatial_400_trend_1958_b.nc &
#cdo -z zip -trend -selyear,1958/2010 ${targetPath}/o2c_mmol_m-3_spatial_700.nc ${targetPath}/o2c_mmol_m-3_spatial_700_trend_1958_a.nc ${targetPath}/o2c_mmol_m-3_spatial_700_trend_1958_b.nc &
#1960-2010 trend μmol kg-1 year-1
#cdo -z zip -trend -selyear,1960/2010 ${targetPath}/o2c_μmol_kg-1_spatial_0_1200.nc ${targetPath}/o2c_μmol_kg-1_spatial_0_1200_trend_1960_a.nc ${targetPath}/o2c_μmol_kg-1_spatial_0_1200_trend_1960_b.nc &
cdo -z zip -trend -selyear,1960/2010 ${targetPath}/o2c_μmol_kg-1_spatial_1200_5000.nc ${targetPath}/o2c_μmol_kg-1_spatial_1200_5000_trend_1960_a.nc ${targetPath}/o2c_μmol_kg-1_spatial_1200_5000_trend_1960_b.nc &


echo "finish"
