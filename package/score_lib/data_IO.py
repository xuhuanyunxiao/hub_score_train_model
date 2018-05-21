# -*- coding: utf-8 -*-
"""
Created on Tue May  8 14:15:38 2018

@author: Administrator
"""

from sqlalchemy import create_engine
from impala.dbapi import connect
from pandas.io import sql
import pandas as pd

#%%
def hive_engine(db_name,                
                user = 'admin', 
                password = 'admin',
                host = '192.168.20.102',
                port = 10000):
    def conn():
        return connect(host = host, port = port, 
                       database = db_name,
                       user = user, password = password,
                       auth_mechanism="PLAIN")
    engine = create_engine('impala://', creator=conn)    
    return engine

def mysql_engine(db_name,
                 user = 'centos7',
                 password = '123456',
                 host = '192.168.30.122',
                 port = 3306):
    DB_CON_STR = 'mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8'%(user, password, 
                                                                host, port, db_name)
    engine = create_engine(DB_CON_STR, echo=False)
    return engine

#%%  
def change_category_to_num(data, to_change_col, changed_col):
    '''
    将分类数据转换成数字，以1为起始值
    cat_num: 类别数量
    '''
    tmp_data = data[[to_change_col, to_change_col]]
#    print(tmp_data.head())
    tmp_data.columns = [to_change_col, changed_col]
#    print(tmp_data.columns)    
    tmp_data[changed_col] = tmp_data[changed_col].astype('category') 
    cat_num = len(tmp_data[changed_col].cat.categories)
    tmp_data[changed_col].cat.categories = [i for i in range(1,  cat_num + 1)]
    tmp_data = tmp_data.drop_duplicates()
    tmp_data.loc[:, changed_col] = tmp_data.loc[:, changed_col].astype(object)
    data = pd.merge(data, tmp_data, how = 'left', on = to_change_col)
    return data 
    
def get_standard_lib_data(data_folder, method = 'mysql'):
    '''
    获取标准库数据，默认：mysql 标准库数据，可选：Excel、hive
    '''

    #db_name = 'standard_lib_mysql'
    #print('db_name 1: ', db_name)
    if method == 'Excel':        
        economic_category_2011 = pd.read_excel(data_folder + '\\economic_category_2011.xlsx',
                                               'economic_category_2011')
        company_type_2011 = pd.read_excel(data_folder + '\\company_type_2011.xlsx',
                                               'company_type_2011')
        prov_dist_county = pd.read_excel(data_folder + '\\prov_dist_county.xlsx',
                                               'prov_dist_county')
    elif method == 'hive':
        pass
    elif method == 'mysql':
        db_name = 'standard_lib_5_mysql'
        engine = create_mysql(db_name)
        economic_category_2011 = sql.read_sql('economic_category_2011', engine).drop('index', axis = 1)
        company_type_2011 = sql.read_sql('company_type_2011', engine).drop('index', axis = 1)
        prov_dist_county = sql.read_sql('prov_dist_county_symbol', engine).drop('index', axis = 1)
  
    # 行业分类   
#    print('  --  economic_category_2011')
    economic_category_2011 = economic_category_2011.drop_duplicates('category_name')
   
    # 公司类型  
#    print('  --  company_type_2011')
    company_type_2011['company_type_name'] = company_type_2011['company_type_name'].\
                                    astype(str).apply(lambda x: x.replace('(','（').replace(')','）'))
    company_type_2011 = company_type_2011.drop_duplicates('company_type_name')    
    # 行政区划    
#    print('  --  district_symbol')
    district = prov_dist_county[['district_symbol','district_name']].drop_duplicates().dropna(how = 'all')
    district['district_symbol'] = district['district_symbol'].apply(lambda x: str(x)[:4] if len(str(x)) == 6 else '错误')

    economic_category_2011 = change_category_to_num(economic_category_2011, 
                                                    'main_category', 'category_class')
    company_type_2011 = change_category_to_num(company_type_2011, 
                                               'company_main_type_name', 'type_class')
    district = change_category_to_num(district, 'district_name', 'district_class')
    
    return economic_category_2011, company_type_2011, district
#%%
def get_table_field_mysql():
    '''
    设定需要使用的表及其字段，包括是否需要计算（衍生变量）
    flag: 1 需要   0 不需要
    table:
    field:
    '''
    # 'company_base_business_merge_new'
    # 'company_branch_new', ## father_company_name  == company_name
    
    table_field = [[0,'company_base_business_merge_new',
                               ["company_regis_capital","company_operat_state",
                                "company_type","company_currency", 
                                "company_registration_time","company_area_code",
                                "company_industry"]],
                    [0,'company_custom_rating',["business_level"]],
                    [0,'company_base_contact_info_new',
                           ["company_email","company_telephone","company_web_site_url"]],
                    [0,'company_imp_exp_credit_info',
                           ["company_annual_report","company_cancellate_no",
                            "company_credit_level",]],
                    [1,'company_business_change_new',["change_time"]],
                    [1,'company_promoters_info_new',["company_initiate_type"]],
                    [1,'company_senior_manager_new',["company_employee_name"]],
                    [1,'company_outbound_investment_new',["investment_company_name"]],
                    [1,'company_execute_persons',["execute_type","execute_perform"]]]   
    
    table_field_list = []
    for [flag, table_name, fields] in table_field:        
        if table_name in ['company_base_business_merge_new']:
            fields.append(['chanle_id','gather_time', 'company_name'])
        else :
            fields.append(['chanle_id','company_gather_time', 'company_name'])
        table_name = 'odm_' + table_name.replace('_new', '')
        fields = str(fields).replace('[', '').replace(']', '').replace("'","")
        table_field_list.append([flag, table_name, fields])
        
    return table_field_list

def get_table_field_hive():
    '''
    设定需要使用的表及其字段，包括是否需要计算（衍生变量）
    flag: 1 需要   0 不需要
    table:
    field:
    '''
    # 'company_base_business_merge_new'
    # 'company_branch_new', ## father_company_name  == company_name
    
    table_field = [[0,'company_base_business_merge_new_bak',
                               ["company_regis_capital","company_operat_state",
                                "company_type","company_currency", 
                                "company_registration_time","company_area_code",
                                "company_industry"]],
                    [0,'company_custom_rating',["business_level"]],
                    [0,'company_base_contact_info_new',
                           ["company_email","company_telephone","company_web_site_url"]],
                    [0,'company_imp_exp_credit_info',
                           ["company_annual_report","company_cancellate_no",
                            "company_credit_level",]],
                    [1,'company_business_change_new',["change_time"]],
                    [1,'company_promoters_info_new',["company_initiate_type"]],
                    [1,'company_senior_manager_new',["company_employee_name"]],
                    [1,'company_outbound_investment',["investment_company_name"]],
                    [1,'company_execute_persons',["execute_type","execute_perform"]]]   # 
    
    table_field_list = []
    for [flag, table_name, fields] in table_field:        
        if table_name in table_field[0]:
            fields.append(['chanle_id','gather_time', 'company_name'])
        else :
            fields.append(['chanle_id','company_gather_time', 'company_name'])
#        table_name = 'odm_' + table_name.replace('_new', '')
        fields = str(fields).replace('[', '').replace(']', '').replace("'","")
        table_field_list.append([flag, table_name, fields])
        
    return table_field_list







