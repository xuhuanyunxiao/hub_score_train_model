#!/usr/bin/python
# -*- coding: utf-8 -*-

#%%
import os
import sys
import pandas as pd
import time
from sklearn.externals import joblib
from pandas.io import sql
  
#%%
def get_time_diff(t1, t2):
    t = t2 - t1
    if t <= 60:
        return '%0.3f s'%t
    elif 60 < t <= 3600:
        t = t /60
        return '%0.3f min'%t
    else :
        t = t /3600
        return '%0.3f h'%t

def get_name_score(data_folder, table_data_commom, i, to_mysql = True):
    # 导入参数及模型
    parameter_path = data_folder + '\\preprocess_parameter.pkl'
    preprocess_parameter = joblib.load(parameter_path)
    
    model_name = 'GradientBoostingRegressor'
    model_path = data_folder + '\\%s.pkl'%model_name 
    regs = joblib.load(model_path)
    
    # 缺失值
    table_data_commom['exist_days'] = table_data_commom['exist_days'].fillna(preprocess_parameter['exist_days_mean'])
    table_data_commom = table_data_commom.fillna(0)
    
    # 标准化
    table_data_commom['exist_days'] = table_data_commom['exist_days'].apply(lambda x: ml_train_test.MinMax(x, 
                                                                           preprocess_parameter['exist_days_min'], 
                                                                           preprocess_parameter['exist_days_max']))
    # 模型预测数据
    company_name = table_data_commom['company_name']
    model_data = table_data_commom.copy()
    for col in ['company_name', 'chanle_id']:
        if col in model_data.columns:
            model_data = model_data.drop(col, axis = 1)
    model_data = model_data.astype(float)
    
    # 模型预测    
    company_score = regs.predict(table_data_commom.drop(['company_name', 'chanle_id'], axis = 1))
    company_score = pd.Series(company_score, name = 'company_score')    
    company_name_score = pd.DataFrame([company_name, company_score]).T
    
    # 结果保存：mysql、csv
    db_name = 'fdm_3_mysql'
    table_name = 'company_name_score'
    save_filename = os.path.join(data_folder, '%s.csv'%table_name)
    
    # mysql
    if to_mysql:
        engine = data_IO.mysql_engine(db_name)       
        if i == 1: sql.execute('drop table if exists %s'%table_name, engine)            
        sql.to_sql(company_name_score, 'company_name_score', engine, 
                   schema=db_name, if_exists='append') 
    
    # csv
    if i == 1:      
        if os.path.exists(save_filename):
            os.remove(save_filename)     
    company_name_score.to_csv(save_filename, index = False,
                              mode = 'a', encoding = 'utf-8') # 追加数据
        
#%%
if __name__ == "__main__":
    time_list = {'t%s'%i:[] for i in range(9)}
    t0 = time.time()
    time_list['t0'].append(t0)
    print("current_file = %s" % __file__)
    pwd = os.path.dirname(os.path.realpath(__file__))
    data_folder = os.path.join(os.path.dirname(pwd), 'data')
    print('当前工作路径：', pwd)
    print('数据文件路径：', data_folder)
    sys.path.append(pwd)
    
    hive_mysql = 'hive'
#    hive_mysql = 'mysql'

    from score_lib import data_IO
    from score_lib import process_data
    from score_lib import ml_train_test
    
    # methon = 'mysql'   'Excel'  'hive'
    economic_category_2011, company_type_2011, district = data_IO.get_standard_lib_data(data_folder, 
                                                                                        method = 'Excel')
    chunksize = 100000  
    if hive_mysql == 'mysql':
        db_name = 'odm_1_mysql'
        engine = data_IO.mysql_engine(db_name)
        table_field_list = data_IO.get_table_field_mysql()
    elif hive_mysql == 'hive':
        db_name = 'data_analysis'
        engine = data_IO.hive_engine(db_name)
#        cursor = data_IO.create_hive() 
#        cursor.execute("use "+ db_name) 
        table_field_list = data_IO.get_table_field_hive()
    
    sql_1 = 'SELECT count(0) FROM %s'%table_field_list[0][1]
    count = pd.read_sql_query(sql_1, engine)
    loop = int(list(count.values)[0] / chunksize) + 1
        
    sql_base = 'SELECT %s FROM %s limit 1000000'%(table_field_list[0][2], table_field_list[0][1])
#    
    i = 0
    for tmp_data in pd.read_sql_query(sql_base, engine, chunksize = chunksize):
        if tmp_data.shape[0] == 0:
            break
        
        i += 1        
        print('--  一、共 %s次循环，第 %s 次获取数据，开始...'%(loop, i))
        t1 = time.time()
        time_list['t1'].append(t1)
        base_data = process_data.get_base_data(tmp_data, table_field_list, 
                                               hive_mysql, engine)
        t2 = time.time()
        time_list['t2'].append(t2)
        print('--  一、共 %s次循环，第 %s 次获取数据%s，结束. 费时：%s'%(loop, i, base_data.shape, get_time_diff(t1, t2)))
        print()
        
        print('--  二、共 %s次循环，第 %s 次预处理数据，开始...'%(loop, i))
        t3 = time.time()   
        time_list['t3'].append(t3)
        table_data_commom = process_data.preprocessing_data(base_data, district,
                                                            economic_category_2011, 
                                                            company_type_2011)
        t4 = time.time()
        time_list['t4'].append(t4)
        print('--  二、共 %s次循环，第 %s 次预处理数据%s，结束. 费时：%s'%(loop, i, table_data_commom.shape, get_time_diff(t3, t4)))
        print()
        
        model_train = False
        if i == 0: model_train = True            
        if model_train:
            print('--  预训练测试模型，开始...')
            t5 = time.time()
            time_list['t5'].append(t5)
            ml_train_test.create_model(table_data_commom, data_folder)
            t6 = time.time()
            time_list['t6'].append(t6)
            print('--  预训练测试模型，结束. 费时：%s'%(get_time_diff(t5, t6)))
            print()        
        
        print('--  三、共 %s次循环，第 %s 次预测结果处理，开始...'%(loop, i))
        t7 = time.time()
        time_list['t7'].append(t7)
        get_name_score(data_folder, table_data_commom, i, to_mysql = True)
        t8 = time.time()
        time_list['t8'].append(t8)
        print('--  三、共 %s次循环，第 %s 次预测结果处理，结束. 费时：%s'%(loop, i, get_time_diff(t7, t8)))
        print()
        

    t00 = time.time()
    time_list['t00'] = t00
    print('--  四、预测结果处理，结束. 费时：%s'%(str(i), get_time_diff(t0, t00)))


      