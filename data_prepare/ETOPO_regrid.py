import time
from osgeo import gdal

def resample(in_tiff, out_tiff, out_res, resample_method):
    """
    inTiff: 输入tif
    output：输出tif
    outRes: 输出tif的分辨率
    resample_method: 重采样方法
    """
    start = time.time()  # 开始计时

    src = gdal.Open(in_tiff)
    if src is None:
        print("打开tif文件失败！")
    else:
        print("成功打开tif文件！")

    tif_prj = src.GetProjection()
    EPSG = 'EPSG:4326'

    gdal.Warp(out_tiff, src, dstSRS=EPSG, xRes=out_res, yRes=out_res, resampleAlg=resample_method)
    print("%s重采样完成！" % out_tiff)

    seconds = time.time() - start  # 计时结束，单位为秒
    m, s = divmod(seconds, 60)
    print(f"运行时间为：%2d分%2d秒" % (m, s))


if __name__ == '__main__':
    in_tiff = "D:\DO_reposity\ETOPO\ETOPO1_Bed_g_geotiff.tif"
    out_tiff = "D:\DO_reposity\ETOPO\ETOPO1_Bed_resample_geotiff.tif"
    resample_method = gdal.GRA_Cubic
    resample_resolution = 0.5
    resample(in_tiff, out_tiff, resample_resolution, resample_method)
    
