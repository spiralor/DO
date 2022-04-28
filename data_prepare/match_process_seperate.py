import os
import psycopg2
import sys
import time

# db_f = sys.argv[1]
db_f = '2db_v0_i0_s1'
# b_f = sys.argv[2]
b_f = 'b0'
conn = psycopg2.connect(database="do", user="postgres", password="1q!@hyes0913", host="localhost",
                        port="5432")
cur = conn.cursor()

# after grid and interpolate table
init_table = 'GRIDDING_{0}_grid_intlev'.format(db_f)
# init_table = 'GRIDDING_{0}_grid_intlev'.format(db_f)
# target talbe, do all operation on it

target_table = 'GRIDDING_{0}'.format(db_f)
# table formate
table_format = 'GRIDDING_data_merge_format'
# 2db path
target_path = db_f

# grid and interpolate
# manual operate to table, XXX_grid_intlev


# # change longitude
# lon_sql = "update \"{0}\" set \"Longitude\"=\"Longitude\"::float-180;\n"\
#     "update \"{0}\" set \"Longitude\"=\"Longitude\"::float+360 where \"Longitude\"::float<0".format(init_table)
# cur.execute(lon_sql)
# conn.commit()
# print("change lon done!\n")
# print(time.asctime(time.localtime(time.time())))
#
# # # match
# # os.system('python matchData4D_salt.py {0}'.format(init_table))
# # os.system('python matchData4D_ssh.py {0}'.format(init_table))
# # os.system('python matchData4D_mv.py {0}'.format(init_table))
# # os.system('python matchData4D_vv.py {0}'.format(init_table))
# # os.system('python matchData4D_zv.py {0}'.format(init_table))
# # os.system('python matchData4D_taux.py {0}'.format(init_table))
# # os.system('python matchData4D_tauy.py {0}'.format(init_table))
# # os.system('python matchData4D_temp.py {0}'.format(init_table))
# # os.system('python matchingData_CMIP6_HR.py {0}'.format(init_table))
# # os.system('python matchETOPO.py {0}'.format(init_table))
# # os.system('python matchSODA09-10.py {0}'.format(init_table))
#
# print('var_X')
# os.system('python matchDatX.py {0}'.format(init_table))
# print('woa13')
# os.system('python compare_woa13_year.py {0}'.format(init_table))
# print('sea_num')
# os.system('python matchData4d_sea_region.py {0}'.format(init_table))
# # print('sea_ice')
# # # os.system('python compare_ice.py {0}'.format(init_table))
#
# count_sql = "select count(*) from \"{0}\"".format(init_table)
# cur.execute(count_sql)
# result = cur.fetchall()
# print("match data done!\n", result, "data matched")
# print(time.asctime(time.localtime(time.time())))
#
# # merge
# create_table_sql = "drop table if exists \"{0}\"; " \
#                        "create table \"{0}\" ( like \"{1}\" INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES );"\
#                         .format(target_table, table_format)
# cur.execute(create_table_sql)
# conn.commit()
#
# # merge_sql = "insert into \"{0}\"(\"Year\",\"Month\",\"Longitude\",\"Latitude\",\"depth(m)\",\"Oxy_mean\",\"Oxy_median\",u,v,w,\"temp\","\
# #             "salt,ssh,taux,tauy,\"ETOPO1_Bed\",\"o2_CMIP6\",woa13_year)"\
# #             "("\
# #             "SELECT \"{1}\".\"Year\",\"{1}\".\"Month\",\"{1}\".\"Longitude\",\"{1}\".\"Latitude\",\"{1}\".\"depth(m)\",\"{1}\".\"Oxy_mean\",\"{1}\".\"Oxy_median\",zv_u.u,mv_v.v,vv_w.w,\"temp\".\"temp\",salt.salt,ssh.ssh,taux.taux,tauy.tauy,etopo.\"ETOPO1_Bed\",\"cmip6\".\"o2_CMIP6\",woa13_year_avg "\
# #             "FROM \"{1}\""\
# #             "LEFT OUTER JOIN \"cmip6\" ON \"{1}\".\"Year\"::float=\"cmip6\".\"Year\"::float and \"{1}\".\"Month\"::float=\"cmip6\".\"Month\"::float and \"{1}\".\"Longitude\"::float=\"cmip6\".\"Longitude\"::float and \"{1}\".\"Latitude\"::float=\"cmip6\".\"Latitude\"::float and \"{1}\".\"depth(m)\"::float=\"cmip6\".\"depth(m)\"::float"\
# #             " LEFT OUTER JOIN etopo ON \"{1}\".\"Year\"::float=\"etopo\".\"Year\"::float and \"{1}\".\"Month\"::float=\"etopo\".\"Month\"::float and \"{1}\".\"Longitude\"::float=\"etopo\".\"Longitude\"::float and \"{1}\".\"Latitude\"::float=\"etopo\".\"Latitude\"::float and \"{1}\".\"depth(m)\"::float=\"etopo\".\"depth(m)\"::float"\
# #             " LEFT OUTER JOIN mv_v ON \"{1}\".\"Year\"::float=\"mv_v\".\"Year\"::float and \"{1}\".\"Month\"::float=\"mv_v\".\"Month\"::float and \"{1}\".\"Longitude\"::float=\"mv_v\".\"Longitude\"::float and \"{1}\".\"Latitude\"::float=\"mv_v\".\"Latitude\"::float and \"{1}\".\"depth(m)\"::float=\"mv_v\".\"depth(m)\"::float"\
# #             " LEFT OUTER JOIN salt ON \"{1}\".\"Year\"::float=\"salt\".\"Year\"::float and \"{1}\".\"Month\"::float=\"salt\".\"Month\"::float and \"{1}\".\"Longitude\"::float=\"salt\".\"Longitude\"::float and \"{1}\".\"Latitude\"::float=\"salt\".\"Latitude\"::float and \"{1}\".\"depth(m)\"::float=\"salt\".\"depth(m)\"::float"\
# #             " LEFT OUTER JOIN ssh ON \"{1}\".\"Year\"::float=\"ssh\".\"Year\"::float and \"{1}\".\"Month\"::float=\"ssh\".\"Month\"::float and \"{1}\".\"Longitude\"::float=\"ssh\".\"Longitude\"::float and \"{1}\".\"Latitude\"::float=\"ssh\".\"Latitude\"::float and \"{1}\".\"depth(m)\"::float=\"ssh\".\"depth(m)\"::float"\
# #             " LEFT OUTER JOIN taux ON \"{1}\".\"Year\"::float=\"taux\".\"Year\"::float and \"{1}\".\"Month\"::float=\"taux\".\"Month\"::float and \"{1}\".\"Longitude\"::float=\"taux\".\"Longitude\"::float and \"{1}\".\"Latitude\"::float=\"taux\".\"Latitude\"::float and \"{1}\".\"depth(m)\"::float=\"taux\".\"depth(m)\"::float"\
# #             " LEFT OUTER JOIN tauy ON \"{1}\".\"Year\"::float=\"tauy\".\"Year\"::float and \"{1}\".\"Month\"::float=\"tauy\".\"Month\"::float and \"{1}\".\"Longitude\"::float=\"tauy\".\"Longitude\"::float and \"{1}\".\"Latitude\"::float=\"tauy\".\"Latitude\"::float and \"{1}\".\"depth(m)\"::float=\"tauy\".\"depth(m)\"::float"\
# #             " LEFT OUTER JOIN \"temp\" ON \"{1}\".\"Year\"::float=\"temp\".\"Year\"::float and \"{1}\".\"Month\"::float=\"temp\".\"Month\"::float and \"{1}\".\"Longitude\"::float=\"temp\".\"Longitude\"::float and \"{1}\".\"Latitude\"::float=\"temp\".\"Latitude\"::float and \"{1}\".\"depth(m)\"::float=\"temp\".\"depth(m)\"::float"\
# #             " LEFT OUTER JOIN vv_w ON \"{1}\".\"Year\"::float=\"vv_w\".\"Year\"::float and \"{1}\".\"Month\"::float=\"vv_w\".\"Month\"::float and \"{1}\".\"Longitude\"::float=\"vv_w\".\"Longitude\"::float and \"{1}\".\"Latitude\"::float=\"vv_w\".\"Latitude\"::float and \"{1}\".\"depth(m)\"::float=\"vv_w\".\"depth(m)\"::float"\
# #             " LEFT OUTER JOIN zv_u ON \"{1}\".\"Year\"::float=\"zv_u\".\"Year\"::float and \"{1}\".\"Month\"::float=\"zv_u\".\"Month\"::float and \"{1}\".\"Longitude\"::float=\"zv_u\".\"Longitude\"::float and \"{1}\".\"Latitude\"::float=\"zv_u\".\"Latitude\"::float and \"{1}\".\"depth(m)\"::float=\"zv_u\".\"depth(m)\"::float"\
# #             " LEFT OUTER JOIN woa13_year ON \"{1}\".\"Year\"::float=\"woa13_year\".\"Year\"::float and \"{1}\".\"Month\"::float=\"woa13_year\".\"Month\"::float and \"{1}\".\"Longitude\"::float=\"woa13_year\".\"Longitude\"::float and \"{1}\".\"Latitude\"::float=\"woa13_year\".\"Latitude\"::float and \"{1}\".\"depth(m)\"::float=\"woa13_year\".\"depth(m)\"::float"\
# #             ")".format(target_table, init_table)
#
# merge_sql = "insert into \"{0}\"(\"Year\",\"Month\",\"Longitude\",\"Latitude\",\"depth(m)\",\"Oxy_mean\",\"Oxy_median\",u,v,w,\"temp\","\
#             "salt,ssh,taux,tauy,\"ETOPO1_Bed\",\"o2_CMIP6\",woa13_year,sea_num)"\
#             "("\
#             "SELECT \"{1}\".\"Year\",\"{1}\".\"Month\",\"{1}\".\"Longitude\",\"{1}\".\"Latitude\",\"{1}\".\"depth(m)\",\"{1}\".\"Oxy_mean\",\"{1}\".\"Oxy_median\",x_var.u,x_var.v,x_var.w,x_var.\"temp\",x_var.salt,x_var.ssh,x_var.taux,x_var.tauy,x_var.\"ETOPO1_Bed\",x_var.\"o2_CMIP6\",woa13_year_avg,sea_num.sea_num "\
#             "FROM \"{1}\""\
#             "\nLEFT OUTER JOIN \"x_var\" ON \"{1}\".\"Year\"::float=\"x_var\".\"Year\"::float and \"{1}\".\"Month\"::float=\"x_var\".\"Month\"::float and \"{1}\".\"Longitude\"::float=\"x_var\".\"Longitude\"::float and \"{1}\".\"Latitude\"::float=\"x_var\".\"Latitude\"::float and \"{1}\".\"depth(m)\"::float=\"x_var\".\"depth(m)\"::float"\
#             "\nLEFT OUTER JOIN woa13_year ON \"{1}\".\"Year\"::float=\"woa13_year\".\"Year\"::float and \"{1}\".\"Month\"::float=\"woa13_year\".\"Month\"::float and \"{1}\".\"Longitude\"::float=\"woa13_year\".\"Longitude\"::float and \"{1}\".\"Latitude\"::float=\"woa13_year\".\"Latitude\"::float and \"{1}\".\"depth(m)\"::float=\"woa13_year\".\"depth(m)\"::float" \
#             "\nLEFT OUTER JOIN sea_num ON \"{1}\".\"Year\"::float=\"sea_num\".\"Year\"::float and \"{1}\".\"Month\"::float=\"sea_num\".\"Month\"::float and \"{1}\".\"Longitude\"::float=\"sea_num\".\"Longitude\"::float and \"{1}\".\"Latitude\"::float=\"sea_num\".\"Latitude\"::float and \"{1}\".\"depth(m)\"::float=\"sea_num\".\"depth(m)\"::float" \
#             ")".format(target_table, init_table)
#
# cur.execute(merge_sql)
# conn.commit()
# count_sql = "select count(*) from \"{0}\"".format(target_table)
# cur.execute(count_sql)
# result = cur.fetchall()
# print("merge data done!", result, "data merged")
# print(time.asctime(time.localtime(time.time())))
#
# # # del 2001
# # delete_sql = "delete from \"{0}\" where \"Latitude\"='-35.25' and \"Longitude\"='35.75' and \"Year\"='2001' and \"Month\"='7';"\
# #             "delete from \"{0}\" where \"Latitude\"='-32.75' and \"Longitude\"='48.25' and \"Year\"='2001' and \"Month\"='7';"\
# #             "delete from \"{0}\" where \"Latitude\"='-31.25' and \"Longitude\"='48.75' and \"Year\"='2001' and \"Month\"='7';"\
# #             "delete from \"{0}\" where \"Latitude\"='-19.75' and \"Longitude\"='50.25' and \"Year\"='2001' and \"Month\"='7';"\
# #             "delete from \"{0}\" where \"Latitude\"='-13.25' and \"Longitude\"='51.25' and \"Year\"='2001' and \"Month\"='7';"\
# #             "delete from \"{0}\" where \"Latitude\"='-11.25' and \"Longitude\"='58.25' and \"Year\"='2001' and \"Month\"='8';"\
# #             "delete from \"{0}\" where \"Latitude\"='-10.25' and \"Longitude\"='52.25' and \"Year\"='2001' and \"Month\"='7';".format(target_table)
# # cur.execute(delete_sql)
# # conn.commit()
# # count_sql = "select count(*) from \"{0}\"".format(target_table)
# # cur.execute(count_sql)
# # result = cur.fetchall()
# # print("delete CTD 2001 data done!\n", result, "data left")
# # print(time.asctime(time.localtime(time.time())))
# #
# # # del 1986
# # delete_sql = "delete from \"{0}\" where \"Year\"::float>=1986 and \"Year\"::float<=1986  and woa13_year::float<10000 and \"Latitude\"::float>=70 " \
# #              "and \"Latitude\"::float<=80 and (\"Month\"='1' or \"Month\"='2' or \"Month\"='3') and \"Oxy_median\"::float<200".format(target_table)
# # cur.execute(delete_sql)
# # conn.commit()
# # count_sql = "select count(*) from \"{0}\"".format(target_table)
# # cur.execute(count_sql)
# # result = cur.fetchall()
# # print("delete OSD 1986 data done!\n", result, "data left")
# # print(time.asctime(time.localtime(time.time())))
#
# # del negative, nan
# # delete_sql = "delete from \"{0}\" where ssh is null or ssh::float < -10000000 or \"o2_CMIP6\" is null or \"o2_CMIP6\"::float>100000000 " \
# #              "or \"Oxy_median\"::float < 0 or salt is null or salt::float < -10000000 or u is null or u::float < -10000000 or taux is null " \
# #              "or taux::float < -10000000  or \"temp\" is null or \"temp\"::float < -10000000".format(target_table)
# delete_sql = "delete from \"{0}\" where ssh is null or ssh::float < -10000000 or \"o2_CMIP6\" is null or \"o2_CMIP6\"::float>100000000 " \
#              "or salt is null or salt::float < -10000000 or u is null or u::float < -10000000 or taux is null " \
#              "or taux::float < -10000000  or \"temp\" is null or \"temp\"::float < -10000000".format(target_table)
# cur.execute(delete_sql)
# conn.commit()
# count_sql = "select count(*) from \"{0}\"".format(target_table)
# cur.execute(count_sql)
# result = cur.fetchall()
# print("del nan data done!\n", result, "data left")
# print(time.asctime(time.localtime(time.time())))
#
# # # 3std
# # std_sql = "-- outlier_all flag\n"\
# #             "with reff as ("\
# #             "        select \"avg\"(\"Oxy_median\"::float) as avg_Oxy, stddev(\"Oxy_median\"::float) as stddev_Oxy "\
# #             "        from \"{0}\" "\
# #             ") "\
# #             "update \"{0}\" SET outlier_all_flag = true "\
# #             "from reff "\
# #             "where abs(\"Oxy_median\"::float - avg_Oxy::float) > (3*stddev_Oxy); \n"\
# #             "delete from \"{0}\" where outlier_all_flag = true; \n"\
# #             "-- outlier_year flag\n"\
# #             "with reff as ( "\
# #             "        select \"avg\"(\"Oxy_median\"::float) as avg_Oxy, stddev(\"Oxy_median\"::float) as stddev_Oxy, \"Year\" "\
# #             "        from \"{0}\" GROUP BY \"Year\" "\
# #             ") "\
# #             "update \"{0}\" SET outlier_year_flag = true "\
# #             "from reff "\
# #             "where abs(\"Oxy_median\"::float - avg_Oxy::float) > (3*stddev_Oxy) and \"{0}\".\"Year\" = reff.\"Year\"; \n"\
# #             "delete from \"{0}\" where outlier_year_flag = true; \n"\
# #             "-- outlier_month flag\n "\
# #             "with reff as ( "\
# #             "        select \"avg\"(\"Oxy_median\"::float) as avg_Oxy, stddev(\"Oxy_median\"::float) as stddev_Oxy, \"Year\", \"Month\" "\
# #             "        from \"{0}\" GROUP BY \"Year\", \"Month\" "\
# #             ") "\
# #             "update \"{0}\" SET outlier_month_flag = true "\
# #             "from reff "\
# #             "where abs(\"Oxy_median\"::float - avg_Oxy::float) > (3*stddev_Oxy) and \"{0}\".\"Year\" = reff.\"Year\" and \"{0}\".\"Month\" = reff.\"Month\"; \n"\
# #             "delete from \"{0}\" where outlier_month_flag = true;".format(target_table)
# # # std_sql = "-- outlier_all flag\n"\
# # #             "with reff as ("\
# # #             "        select \"avg\"(\"Oxy_median\"::float) as avg_Oxy, stddev(\"Oxy_median\"::float) as stddev_Oxy "\
# # #             "        from \"{0}\" "\
# # #             ") "\
# # #             "update \"{0}\" SET outlier_all_flag = (abs(\"Oxy_median\"::float - avg_Oxy::float) > (3*stddev_Oxy)) , all_mean = avg_Oxy, all_std = stddev_Oxy "\
# # #             "from reff; \n "\
# # #             "-- outlier_year flag\n"\
# # #             "with reff as ( "\
# # #             "        select \"avg\"(\"Oxy_median\"::float) as avg_Oxy, stddev(\"Oxy_median\"::float) as stddev_Oxy, \"Year\" "\
# # #             "        from \"{0}\" GROUP BY \"Year\" "\
# # #             ") "\
# # #             "update \"{0}\" SET outlier_year_flag = (abs(\"Oxy_median\"::float - avg_Oxy::float) > (3*stddev_Oxy)) , year_mean = avg_Oxy, year_std = stddev_Oxy "\
# # #             "from reff "\
# # #             "where \"{0}\".\"Year\" = reff.\"Year\"; \n"\
# # #             "-- outlier_month flag\n "\
# # #             "with reff as ( "\
# # #             "        select \"avg\"(\"Oxy_median\"::float) as avg_Oxy, stddev(\"Oxy_median\"::float) as stddev_Oxy, \"Year\", \"Month\" "\
# # #             "        from \"{0}\" GROUP BY \"Year\", \"Month\" "\
# # #             ") "\
# # #             "update \"{0}\" SET outlier_month_flag = (abs(\"Oxy_median\"::float - avg_Oxy::float) > (3*stddev_Oxy)) , month_mean = avg_Oxy, month_std = stddev_Oxy "\
# # #             "from reff "\
# # #             "where \"{0}\".\"Year\" = reff.\"Year\" and \"{0}\".\"Month\" = reff.\"Month\"; \n".format(target_table)
# # cur.execute(std_sql)
# # conn.commit()
# # count_sql = "select count(*) from \"{0}\"".format(target_table)
# # cur.execute(count_sql)
# # result = cur.fetchall()
# # print("3std done!\n", result, "data left")
# # print(time.asctime(time.localtime(time.time())))
#
# # min max check
# check_sql = "select min(\"Oxy_median\"::float),max(\"Oxy_median\"::float) from \"{0}\"".format(target_table)
# cur.execute(check_sql)
# conn.commit()
# result = cur.fetchall()
# print(result)
#
# # # sea_range_limit
# # print('sea limit')
# # os.system('python sea_limit.py {0}'.format(target_table))
# # del_sql = "drop table if exists \"{0}\"".format(target_table)
# # cur.execute(del_sql)
# # conn.commit()
# # chname_sql = "alter table \"{0}_limit\" rename to \"{0}\"".format(target_table)
# # cur.execute(chname_sql)
# # conn.commit()
# # # # qc delete data
# # del_sql = "delete from \"{0}\" where " \
# #           "(\"Oxy_median\"::float > sea_num_depth_max_oxy::float) or " \
# #           "(\"Oxy_median\"::float < sea_num_depth_min_oxy::float)"\
# #         .format(target_table)
# # cur.execute(del_sql)
# # conn.commit()
# # count_sql = "select count(*) from \"{0}\"".format(target_table)
# # cur.execute(count_sql)
# # result = cur.fetchall()
# # print("sea_range qc done!\n", result, "data left")
# # print(time.asctime(time.localtime(time.time())))
# # # del_sql = "delete from \"{0}\" where " \
# # #           "(\"Oxy_median\"::float > sea_num_depth_max_oxy::float and \"Oxy_median\"::float > (all_mean::float+3*all_std::float*3)) or " \
# # #           "(\"Oxy_median\"::float < sea_num_depth_min_oxy::float and \"Oxy_median\"::float < (all_mean::float-3*all_std::float*3))"\
# # #         .format(target_table)
# # # cur.execute(del_sql)
# # # conn.commit()
# # # print("sea_range done!")
# # # print(time.asctime(time.localtime(time.time())))
#
#
# # ===================================================================================================

# balance data
# spatial id
create_table_sql = "drop table if exists \"{0}_sid\"; " \
                       "create table \"{0}_sid\" ( like \"{1}\" INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES );"\
                        .format(target_table, table_format)
cur.execute(create_table_sql)
conn.commit()
spatial_id_sql = "insert into \"{0}_sid\""\
                "\n select \"Year\",\"Month\",\"Longitude\",\"Latitude\",\"depth(m)\",\"Oxy_mean\",\"Oxy_median\",u,v,w,\"temp\",salt,ssh,taux,tauy,\"ETOPO1_Bed\",\"o2_CMIP6\",o2sat,"\
                "\n dense_rank() over (order by \"Longitude\"::float desc, \"Latitude\"::float desc, \"depth(m)\"::float desc) as station_id," \
                "\n station_count,woa13_year,outlier_all_flag,outlier_month_flag,outlier_year_flag, sea_num from \"{0}\"".format(target_table)
cur.execute(spatial_id_sql)
conn.commit()
del_sql = "drop table if exists \"{0}\"".format(target_table)
cur.execute(del_sql)
conn.commit()
chname_sql = "alter table \"{0}_sid\" rename to \"{0}\"".format(target_table)
cur.execute(chname_sql)
conn.commit()

count_sql = "select count(*) from \"{0}\"".format(target_table)
cur.execute(count_sql)
result = cur.fetchall()
print("spatial id done!\n", result, "data left")
print(time.asctime(time.localtime(time.time())))


os.system('python data_year.py {0}'.format(target_table))
count_sql = "select count(*) from \"{0}\"".format(target_table+'_b_seed1')
cur.execute(count_sql)
result = cur.fetchall()
print("balance year data done!\n", result, "data left")
print(time.asctime(time.localtime(time.time())))

if b_f == 'b0':
    os.system('python balance_spatial.py {0}'.format(target_table))
    count_sql = "select count(*) from \"{0}\"".format(target_table+'_b0_seed1')
    cur.execute(count_sql)
    result = cur.fetchall()
    print("balance spatial data done!\n", result, "data left")
    print(time.asctime(time.localtime(time.time())))
elif b_f == 'b2':
    print(target_table)
    os.system('python balance_spatial_chn_north.py {0}'.format(target_table))
    count_sql = "select count(*) from \"{0}\"".format(target_table+'_b2_seed1')
    cur.execute(count_sql)
    result = cur.fetchall()
    print("balance spatial data done!\n", result, "data left")
    print(time.asctime(time.localtime(time.time())))

# output
if os.path.exists('E:\do4\do_train\dataset\\2db\\{0}'.format(db_f)) == False:
    os.mkdir('E:\do4\do_train\dataset\\2db\\{0}'.format(db_f))
os.system('python output_data.py {0} {1} {2}'.format(target_table, target_path, b_f))
print("output data done!")
print(time.asctime(time.localtime(time.time())))

conn.close()
