# -*- coding: utf-8 -*-
"""
Created on Thu May 17 11:15:40 2018

@author: Administrator
"""

#%%
import numpy as np 
import pandas as pd

from score_lib import cleaning_data
from score_lib import data_IO

from impala.util import as_pandas

names = locals()

#%%
def get_base_data(tmp_data, table_field_list, 
                  hive_mysql, engine): 
    data_shape = []
    record_changes = []
#    base_data = tmp_data  
    base_data = cleaning_data.common_clean_step(table_field_list[0][1], 
                                     tmp_data, record_changes, id_name = True)    
    chanle_id_list = base_data['chanle_id'].tolist()
    data_shape.append(['0 base_data: ', base_data.shape])
    
    for index, [flag, table_name, fields] in enumerate(table_field_list):
        if index > 0:
            sql_c = 'SELECT %s FROM %s where chanle_id in (%s)' %(fields, table_name, chanle_id_list)
            sql_c = sql_c.replace('[', '').replace(']', '')
            print('--  ', table_name)
            try :
                names['%s'%table_name] = pd.read_sql_query(sql_c, engine)
                if hive_mysql == 'hive':
#                    cursor.execute(sql_c)
#                    names['%s'%table_name] = as_pandas(cursor)
#                    names['%s'%table_name].columns = [x.strip() for x in table_field_list[index][2].split(',')]                    
                    combined_col = 'chanle_id'
                    droped_col = 'company_name'
                elif hive_mysql == 'mysql':                
                    combined_col = 'company_name'
                    droped_col = 'chanle_id'
                
                # 处理空数据情况
                if names['%s'%table_name].shape[0] == 0:
                    print(' -- 处理空数据情况')
                    print(names['%s'%table_name].shape)
                    names['%s'%table_name].loc[0, 'chanle_id'] = base_data.loc[base_data.index.tolist()[0], 'chanle_id']
                    names['%s'%table_name] = names['%s'%table_name].fillna(0)
                    print(names['%s'%table_name].shape)
                    
                record_changes = [] # 每个阶段的数据量及特征量     
                if flag == 0:                                    
                    names['%s'%table_name] = cleaning_data.common_clean_step(table_name, 
                                                     names['%s'%table_name], 
                                                     record_changes, id_name = True)
                elif flag == 1:
                    names['%s'%table_name] = cleaning_data.common_clean_step(table_name, 
                                                     names['%s'%table_name], 
                                                     record_changes, id_name = False)    
                    tmp_field = table_name.replace('odm_company_', '') + '_num'
                    names['%s'%tmp_field] = names['%s'%table_name].groupby(['company_name'])['chanle_id'].count()
                    names['%s'%tmp_field] = names['%s'%tmp_field].reset_index() # Series to DataFrame,索引变列
                    names['%s'%tmp_field].columns = ['company_name',tmp_field]
                    names['%s'%table_name] = names['%s'%table_name].drop_duplicates(subset = 'company_name')                   
                    names['%s'%table_name] = pd.merge(names['%s'%table_name], 
                                                      names['%s'%tmp_field], 
                                                      on = 'company_name', how = 'left')  
                    
                drop_col_list = ['gather_time', droped_col, #  'company_name', chanle_id
                                 'index', 'company_gather_time']
                for col in drop_col_list:
                    if col in names['%s'%table_name].columns:
                        names['%s'%table_name] = names['%s'%table_name].drop(col, axis = 1)

                base_data = pd.merge(base_data, names['%s'%table_name],
                                      how = 'left', on = combined_col) #  'company_name', chanle_id
                base_data = base_data.drop_duplicates(combined_col) #  'company_name', chanle_id
                data_shape.append(['base_data & %s: '%table_name, 
                                   base_data.shape, 
                                   names['%s'%table_name].shape])
                
                if flag == 1: 
                    base_data[tmp_field] = base_data[tmp_field].fillna(0)
                
                del names['%s'%table_name]

            except Exception as e:
                print('---------------  错误： ---')
                print(e)
                print(table_name)
                print('-------------------------')
            
    base_data = base_data.fillna('EEEEE') 
    data_shape.append(['1 base_data: ', base_data.shape])
#    for s in data_shape:print(s)
    
    return base_data   
#%%
def preprocessing_data(base_data, district,economic_category_2011,company_type_2011):        
    # 符号值处理
    table_data_commom = base_data.applymap(cleaning_data.handle_punc)
    
    # 时间值处理
    table_data_commom['company_registration_time'] = table_data_commom['company_registration_time'].\
                                                            replace('EEEEE',np.nan)
    
#    for index in table_data_commom['company_registration_time'].index:
#        x  = table_data_commom['company_registration_time'][index]
#        try :
#            pd.to_datetime(x)
#        except Exception as e :
#            print('company_registration_time  ', e)
#            print(x)
#            print(index)
#            y = cleaning_data.get_correct_date(x)
#            print(y)
#            table_data_commom.loc[index, 'company_registration_time'] = y
#    
#    table_data_commom['company_registration_time'] = pd.to_datetime(table_data_commom['company_registration_time'])    
    
    table_data_commom['company_registration_time'] = table_data_commom['company_registration_time'].\
                                                            apply(cleaning_data.get_correct_date)
        
    # # 衍生变量：公司存在时间
    print('  --  exist_days')
    table_data_commom['gather_time'] = pd.to_datetime(table_data_commom['gather_time'])
    table_data_commom['exist_days'] = table_data_commom['gather_time'] \
                                            - table_data_commom['company_registration_time']
    table_data_commom['exist_days'] = table_data_commom['exist_days'].apply(lambda x:x.days)       
   
    # 公司状态   
    print('  --  company_operat_state')
    table_data_commom['company_operat_state'] = table_data_commom['company_operat_state'].\
                                                    apply(cleaning_data.get_correct_state)
          
    # 行业分类  
    print('  --  category_name')
    table_data_commom = pd.merge(table_data_commom, economic_category_2011,
                                 how = 'left', left_on = 'company_industry', 
                                 right_on = 'category_name')
    
    # 行政区划    
    print('  --  district_name')
    table_data_commom['company_area_code'] = table_data_commom['company_area_code'].apply(lambda x: str(x)[:4])        
    table_data_commom = pd.merge(table_data_commom, district,
                                 how = 'left', left_on = 'company_area_code',
                                 right_on = 'district_symbol')                                                                 
            
    # 注册资本
    print('  --  company_regis_capital')
    table_data_commom['company_regis_capital_class'] = table_data_commom['company_regis_capital'].\
                                                apply(lambda x:cleaning_data.get_regis_class(x))

    #% 公司类型
    print('  --  company_main_type_name')
    table_data_commom['company_type'] = table_data_commom['company_type'].astype(str).\
                                    apply(lambda x: x.replace('(','（').replace(')','）').replace('台港澳','港澳台'))
    company_type_data = pd.merge(table_data_commom[['company_name', 'company_type']], 
                                 company_type_2011,
                                 how = 'left', left_on = 'company_type', 
                                 right_on = 'company_type_name')
    company_type_data['company_main_type_name'] = company_type_data['company_type'].\
                                                        apply(cleaning_data.get_correct_company_type)        
    table_data_commom = pd.merge(table_data_commom, 
                                 company_type_data[['company_name', 'type_class', 
                                                   'company_main_type_name']],
                                 how = 'left', on = 'company_name')                      
            
    # 邮箱、电话、网址: 有 1 无 0
    print('  --  email/telephone/web_site')
    table_data_commom['is_email'] = table_data_commom['company_email'].\
                                        apply(lambda x: 0 if x == 'EEEEE' else 1)
    table_data_commom['is_telephone'] = table_data_commom['company_telephone'].\
                                        apply(lambda x: 0 if x == 'EEEEE' else 1)
    table_data_commom['is_web_site'] = table_data_commom['company_web_site_url'].\
                                        apply(lambda x: 0 if x == 'EEEEE' else 1)
                                        
    # execute_type	类型（未知(0)、自然人(1)、企业(2)）
    # execute_perform	被执行人的履行情况：未知(0)、部分未履行(1)、全部未履行(2)
    table_data_commom['execute_type'] = table_data_commom['execute_type'].replace('0','自然人').replace('1','企业')
    table_data_commom['execute_type'] = table_data_commom['execute_type'].\
                                            apply(cleaning_data.get_execute_type)
    table_data_commom['execute_perform'] = table_data_commom['execute_perform'].\
                                            apply(cleaning_data.get_execute_perform)
                                        
    # company_annual_report	年报情况
    # company_cancellate_no	海关注销标志        
    # company_credit_level	信用等级 
    table_data_commom['company_annual_report'] = table_data_commom['company_annual_report'].\
                                            apply(cleaning_data.get_annual_report)        
    table_data_commom['company_cancellate_no'] = table_data_commom['company_cancellate_no'].\
                                            apply(cleaning_data.get_cancellate_no)        
    table_data_commom['company_credit_level'] = table_data_commom['company_credit_level'].\
                                            apply(cleaning_data.get_credit_level)        
    
    # business_level
    table_data_commom['business_level'] = table_data_commom['business_level'].\
                                            apply(cleaning_data.get_business_level)
                                            
    drop_field_list = ['company_registration_time', 'gather_time',
                       'company_type', 'company_regis_capital', 
                       'company_area_code', 'company_industry',
                       'company_email', 'company_telephone','company_web_site_url', 
                       'change_time', 'company_currency',
                       'investment_company_name', 'company_initiate_type',
                       'company_employee_name', 
                       'category_code','category_name','main_category',
                       'company_main_type_name','district_name','district_symbol',
                       ]
    table_data_commom = table_data_commom.drop(drop_field_list, axis = 1)  
    
    return table_data_commom








