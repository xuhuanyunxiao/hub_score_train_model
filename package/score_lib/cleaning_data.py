# -*- coding: utf-8 -*-
"""
Created on Tue May  8 15:32:26 2018

@author: Administrator
"""

import pandas as pd
import numpy as np
import re

def common_clean_step(table_name, table_data, 
                      record_changes, id_name = True):
    '''
    各表共同步骤:非空字段、空值、采集时间、去重
    1 如果首行是表头则去除
    2 空值:'company_name','chanle_id' 均不为空
    3 采集时间处理：先按gather_time排序，后面去重取最新数据
    4 去重（可选:id_name = True）：'company_name','chanle_id' 重复
    5 填补空值：‘EEEEE’
    '''
    
    # 如果首行是表头则去除
    if table_data.iloc[0,-1] == table_data.columns.tolist()[-1]:
        table_data = table_data.drop(0, axis = 0)
        
    # 原始数据情况记录
    record_changes.append([table_name,"0 原始数据", table_data.shape])
    
    # 空值   # 'company_name','chanle_id' 均不为空
    try :
        table_data = table_data[table_data['company_name'].notnull()]
        table_data = table_data[table_data['chanle_id'].notnull()]
        record_changes.append([table_name,"1 公司名、id 均不为空", table_data.shape])        
    except :
        table_data = table_data[table_data['chanle_id'].notnull()]        
        record_changes.append([table_name,"1 id 不为空", table_data.shape])
    
    # 采集时间处理：先按gather_time排序，后面去重取最新数据
    if 'gather_time' in table_data.columns.tolist():
        col_name = 'gather_time'
    else :
        col_name = 'company_gather_time'
    for index in table_data.index:
        try :
            pd.to_datetime(table_data[col_name][index])
        except :
            print(col_name, ' -- ', index, ' -- ', 
                  table_data[col_name][index])
            table_data[col_name][index] = np.nan
            continue
        
    table_data[col_name] = pd.to_datetime(table_data[col_name])
    table_data = table_data.sort_values(by = col_name, 
                                        ascending = False, 
                                        na_position = 'last')

    # 所有字段均重复
    table_data = table_data[~table_data.duplicated()] 
    record_changes.append([table_name,"2 所有字段均重复", table_data.shape])
    
    # 'company_name','chanle_id' 重复
    if id_name:
        # 有些情况下不能根据这两者去重，例如高管信息、分支信息等，默认处理        
        table_data = table_data[~table_data.duplicated('company_name')] 
        table_data = table_data[~table_data.duplicated('chanle_id')]              
        record_changes.append([table_name,"3 公司名 id 均重复", 
                               table_data.shape])

    # 采集时间不为空
    shapes = table_data[table_data[col_name].isnull()].shape
    value = table_data[col_name].value_counts().idxmax()
    table_data[col_name] = table_data[col_name].fillna(value) 
    record_changes.append([table_name,"4 采集时间不为空", value, shapes,
                           table_data.shape])
    
    #% 填补空值：
    table_data = table_data.fillna('EEEEE').replace('暂无', 'EEEEE') 
    table_data = table_data.applymap(lambda x: x.replace('', 'EEEEE') if len(str(x)) == 0 else x)
    
#    for record in record_changes:
#        print('  ***  ', record)

    return table_data 

def handle_punc(x):
    '''
    处理 ‘’、‘-’、‘***’等情况，以及空值，均替换成：‘EEEEE’
    '''
    x = str(x)
    if (len(x) == 1) & (x == '-'):
        return 'EEEEE'
    elif (len(x) == 3) & (x == '***'):
        return 'EEEEE'
    else :
        if x == '未公开':
            return 'EEEEE'
        else :
            return x

def get_correct_date(x):
    '''
    table: company_base_business_merge_new（企业工商注册）
    field: company_registration_time	注册时间
    value: 
    '''      
    
    try :
        try :
            return pd.to_datetime(x)
        except :
            mat = re.search(r"(\d{4}(年|-)\d{1,2}(月|-)\d{1,2})",x)
            if mat:
                date  = mat.groups(0)[0].replace('年','/').replace('月','/').replace('日','')
    #            print('校正后日期：', date)
                return pd.to_datetime(date)
            else :
                print('-- 无匹配:get_correct_date --')
                print('---', x)
                return np.nan
    except :
        return np.nan

def get_correct_state(x):
    '''
    table: company_base_business_merge_new（企业工商注册）
    field: company_operat_state	经营状态
    value: 未知0  在营5   吊销，未注销2  吊销1  注销3  迁出4
    '''         
    try :
        if re.search(r'\[正常|在营|存续|开业|在业]*', x):
            return 5 # '在营'
        elif re.search(r'\吊销，未注销*', x):
            return 2 # '吊销，未注销'
        elif re.search(r'\吊销*', x):
            return 1 # '吊销'
        elif re.search(r'\注销*', x):
            return 3 # '注销'
        elif re.search(r'\迁出*', x):
            return 4 # '迁出'
        elif re.search(r'\正常*', x):
            return 5 # '在营'    
        else :
            return 0 # x   # 未公开
    except Exception as e:
        print(e)
        print('get_correct_state: ', x)
    
def get_correct_company_type(x):
    '''
    table: company_base_business_merge_new（企业工商注册）
    field: company_type	企业(机构)类型
    value: 
    '''     
    if re.search(r'\个体工商户|个体|其它经济成份联营|民办非企业单位*', x):
        return '内资非法人企业_非公司私营企业_内资非公司企业分支机构'
    elif re.search(r'\农民专业合作经济组织*', x):
        return '其他类型'
    elif re.search(r'\有限责任公司（自然人|一人有限责任公司|有限责任公司（法人独资|其他有限责任公司*', x):
        return '内资公司_有限责任公司'
    elif re.search(r'\其他股份有限公司（非上市）|上市股份有限公司*', x):
        return '内资公司_股份有限公司'    
    elif re.search(r'\股份有限公司分公司（非上市、国有控股）*', x):
        return '内资分公司_股份有限公司'      
    elif re.search(r'\联营（法人）|集体所有制（股份合作）|全民（内联）|机关法人*', x):
        return '内资企业法人'
    elif re.search(r'\外商投资企业分公司|外商投资企业办事处*', x):
        return '外商投资企业_其他'
    elif re.search(r'\有限责任公司（法人独资）（外商投资企业投资）*', x):
        return '外商投资企业_有限责任公司'   
    elif re.search(r'\合资经营（港资）*', x):
        return '港澳台投资企业_其他'    
    else :
        return x   # 未公开
        
def get_area(x, district):
    '''
    table: company_base_business_merge_new（企业工商注册）
    field: company_area_code	地区行政编码
    value: 
    '''      
    if x.isdigit():
        if len(x) == 4:
            xx = district[district['district_symbol'].astype(str).str.contains(r'^%s'%x)]['district_name'].tolist()
            if xx:
                return xx[0]
            else :
                return np.nan   
    else :
        return np.nan  

def get_regis_class(x):
    '''
    table: company_base_business_merge_new（企业工商注册）
    field: company_regis_capital	注册资本(万元)
    value: [10, 50, 100, 500, 1000, 5000, 10000, 2000, 50000]
    '''    
    
    num_list = [10, 50, 100, 500, 1000, 5000, 10000, 2000, 50000]
    try :
        x = float(x)
        if x :
            if x <= num_list[0]: 
                return 1
            elif num_list[0] < x <= num_list[1]:
                return 2 
            elif num_list[1] < x <= num_list[2]:
                return 3    
            elif num_list[2] < x <= num_list[3]:
                return 4    
            elif num_list[3] < x <= num_list[4]:
                return 5    
            elif num_list[4] < x <= num_list[5]:
                return 6    
            elif num_list[5] < x <= num_list[6]:
                return 7    
            elif num_list[6] < x <= num_list[7]:
                return 8    
            elif num_list[7] < x <= num_list[8]:
                return 9    
            else :
                return 10
    except Exception as e:
        print(e)
        print('get_regis_class   x：',x)
        print('type(x)：',type(x))
        return 0

def get_execute_type(x):
    '''
    table: company_execute_persons（失信被执行人信息）
    field: execute_type	类型
    value: （未知(0)、自然人(1)、企业(2)）
    '''
    if x == '自然人':
        return 1
    elif x == '企业':
        return 2
    else :
        return 0

def get_execute_perform(x):
    '''
    table: company_execute_persons（失信被执行人信息）
    field: execute_perform	被执行人的履行情况
    value: 未知(0)、部分未履行(1)、全部未履行(2)
    '''    
    if '全部未履行' in x:
        return 2
    if '部分未履行' in x:
        return 1    
    else :
        return 0

def get_annual_report(x):
    '''
    table: company_imp_exp_credit_info（进出口信用信息）
    field: company_annual_report	年报情况
    value: 未报送	2  已报送	4  未知	0  超期未报送	1  超期报送	3  不需要	5
    '''       
    if '不需要' in x :
        return 5
    elif '已报送' in x :
        return 4
    elif '超期报送' in x :
        return 3
    elif '未报送' in x :
        return 2
    elif '超期未报送' in x :
        return 1
    else :
        return 0
    
def get_cancellate_no(x):
    '''
    table: company_imp_exp_credit_info（进出口信用信息）
    field: company_cancellate_no	海关注销标志
    value: 正常 2    注销 1  未知	 0 
    '''        
    if '正常' in x :
        return 2
    elif '注销' in x :
        return 1
    else :
        return 0    
    
def get_credit_level(x):
    '''
    table: company_imp_exp_credit_info（进出口信用信息）
    field: company_credit_level	信用等级
    value: 一般信用企业 2    一般认证企业 3    高级认证企业 4   失信企业 1   未知	0  
    '''      
    if '高级认证企业' in x :
        return 4
    elif '一般认证企业' in x :
        return 3
    elif '一般信用企业' in x :
        return 2
    elif '失信企业' in x :
        return 1
    else :
        return 0    

def get_business_level(x):
    '''
    table: company_custom_rating（海关评级信息）
    field: business_level	海关评级
    value: C 2    B 3    A 4   D 1   未知	0  
    
    # 海关评级应为ABCD四个等级，但是大库中也有0,1,3，且数量不少。
    # 暂时将0-3对应ABCD    
    '''     
    x = str(x).replace('0', 'A').replace('1', 'B').replace('2', 'C').replace('3', 'D')    
    if 'A' in x:
        return 4
    elif 'B' in x:
        return 3    
    elif 'C' in x:
        return 2    
    elif 'D' in x:
        return 1
    else :
        return 0

    
    