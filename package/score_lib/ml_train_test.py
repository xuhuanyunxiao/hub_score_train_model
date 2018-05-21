# -*- coding: utf-8 -*-
"""
Created on Thu May 17 14:08:10 2018

@author: Administrator
"""

#%%
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

from sklearn.model_selection import train_test_split
from sklearn import metrics
from sklearn import model_selection
from sklearn.externals import joblib

from sklearn import ensemble

#%% 获取数据集 + 预处理
def MinMax(x, col_min, col_max):
    '''
    # 区间缩放法 [0, 1]
    '''
    try :
        if str(x) != 'nan':
            y = (x-col_min)/(col_max - col_min)
            return y
        else :
            return x       
    except Exception as e:
        print(e)
        print(x)
        
def get_dataset(table_data_commom, data_folder):
    score_file = data_folder + '\\name_num_20180416.txt'
    fid = open(score_file, 'r+', encoding = 'utf-8')
    score_data = fid.readlines()
    fid.close()
    
    score_data = pd.DataFrame(score_data, columns = ['name_province_score'])
    score_data = score_data.applymap(lambda x: x.replace('\n', ''))
    score_data['company_name'] = score_data.applymap(lambda x: x.split('_')[0])
    score_data['province'] = score_data['name_province_score'].apply(lambda x: x.split('_')[2])
    score_data['score'] = score_data['province'].str.findall(r'\d+')
    score_data['score'] = score_data['score'].apply(lambda x: x[0]).astype(float)
    
    dataset = pd.merge(score_data[['company_name','score']], table_data_commom, 
                            on = 'company_name', how = 'left')
    dataset = dataset[dataset['chanle_id'].notnull()]
    for col in ['company_name', 'chanle_id']:
        if col in dataset.columns:
            dataset = dataset.drop(col, axis = 1)
    
    preprocess_parameter = {}
    # 缺失值处理
    exist_days_mean = dataset['exist_days'].mean()
    dataset['exist_days'] = dataset['exist_days'].fillna(exist_days_mean)
    dataset = dataset.fillna(0)
    
    # 标准化: exist_days
    col_min = dataset['exist_days'].min()
    col_max = dataset['exist_days'].max()
    dataset['exist_days'] = dataset['exist_days'].apply(lambda x: MinMax(x, 
                                                           col_min, col_max))
    
    preprocess_parameter['exist_days_mean'] = exist_days_mean
    preprocess_parameter['exist_days_min'] = col_min
    preprocess_parameter['exist_days_max'] = col_max
    
    dataset = dataset.astype(float)
    
    return dataset, preprocess_parameter

#%% 交叉验证 + 参数寻优
def get_modes_result(name, reg, X, y, X_train, y_train, X_test, y_test):
    regs = reg.fit(X_train, y_train)
    y_predict = regs.predict(X_test)
    y_predict_val = model_selection.cross_val_predict(reg, X, y, cv=10)
    
    #print('原始值：', [int(x) for x in y_test.tolist()[:30]])
    #print('预测值：', [int(x) for x in y_predict[:30]])
    
    print('回归模型：%s'%name)
    print(' -- 平均绝对误差（MAE）：', '%0.4f'%metrics.mean_absolute_error(y_test, y_predict), 
          '%0.4f'%metrics.mean_absolute_error(y, y_predict_val)) 
    print(' -- RMSE：', '%0.4f'%np.sqrt(metrics.mean_squared_error(y_test, y_predict)), 
          '%0.4f'%np.sqrt(metrics.mean_squared_error(y, y_predict_val)))
    print(' -- 平均方差（MSE）：', '%0.4f'%metrics.mean_squared_error(y_test, y_predict), 
          '%0.4f'%metrics.mean_squared_error(y, y_predict_val))    
    print(' -- R平方值：', '%0.4f'%metrics.r2_score(y_test, y_predict), 
          '%0.4f'%metrics.r2_score(y, y_predict_val)) 
    print(' -- 中值绝对误差：', '%0.4f'%metrics.median_absolute_error(y_test, y_predict), 
          '%0.4f'%metrics.median_absolute_error(y, y_predict_val)) 
    
    # 真实值和预测值的变化关系，离中间的直线y=x直接越近的点代表预测损失越低
    #fig, axs = plt.subplots(2,2, figsize = (15,12))
    fig = plt.figure(figsize = (20,18))
    fig.suptitle('%s'%name, fontsize = 25)

    title_fontsize = 20
    label_fontsize = 15
    
    gs = gridspec.GridSpec(3, 2)
    axs1 = plt.subplot(gs[0, 0])
    axs2 = plt.subplot(gs[0, 1])
    axs3 = plt.subplot(gs[1, :])
    axs5 = plt.subplot(gs[2, :])
    
    axs1.scatter(y_test, y_predict)
    axs1.plot([y_test.min(), y_test.max()], 
                [y_test.min(), y_test.max()], 
                'k--', lw=4)
    axs1.set_xlabel('y_test', fontsize = label_fontsize)
    axs1.set_ylabel('y_predict', fontsize = label_fontsize)
    axs1.set_title('test_predict', fontsize = title_fontsize)
    axs1.legend(['y = x'], loc='lower right')  

    axs2.scatter(y, y_predict_val)
    axs2.plot([y.min(), y.max()], 
                [y.min(), y.max()], 
                'k--', lw=4)
    axs2.set_xlabel('y', fontsize = label_fontsize)
    axs2.set_ylabel('y_predict_val', fontsize = label_fontsize)
    axs2.set_title('y_predict_cross_val', fontsize = title_fontsize)    
    axs2.legend(['y = x'], loc='lower right')
    
    axs3.plot(range(len(y_test)), y_test)
    axs3.plot(range(len(y_test)), y_predict)
    axs3.set_xlabel('index', fontsize = label_fontsize)
    axs3.set_ylabel('score', fontsize = label_fontsize)
    axs3.set_title('test_predict', fontsize = title_fontsize)
    axs3.legend(['y_test', 'y_predict'], loc='upper right')
    
    minus = list(map(lambda x: x[0]-x[1], zip(y_test, y_predict))) 
    axs4 = axs3.twinx()
    axs4.plot(range(len(minus)), minus, 'r')
    axs4.axhline(0, xmin = 0.05, xmax = 0.95, linewidth=2, color = 'k', linestyle = '--')
    axs4.set_ylabel('minus value', fontsize = label_fontsize)    
    axs4.legend(['y_test - y_predict', ' y = 0'], loc='lower right')
    axs4.set_ylim([-40, 200])

    axs5.plot(range(len(y)), y)
    axs5.plot(range(len(y_predict_val)), y_predict_val)
    axs5.set_xlabel('index', fontsize = label_fontsize)
    axs5.set_ylabel('score', fontsize = label_fontsize)
    axs5.set_title('y_predict_cross_val', fontsize = title_fontsize)
    axs5.legend(['y', 'y_predict_val'], loc='upper right')
    
    minus = list(map(lambda x: x[0]-x[1], zip(y, y_predict_val))) 
    axs6 = axs5.twinx()
    axs6.plot(range(len(minus)), minus, 'r')
    axs6.axhline(0, xmin = 0.05, xmax = 0.95, linewidth=2, color = 'k', linestyle = '--')
    axs6.set_ylabel('minus value', fontsize = label_fontsize)    
    axs6.legend(['y - y_predict_val', ' y = 0'], loc='lower right')
    axs6.set_ylim([-40, 200])    
    plt.show()
    
    return regs    

#%%
def create_model(table_data_commom, data_folder):
    dataset, preprocess_parameter = get_dataset(table_data_commom, data_folder)
    
    X = dataset.drop('score', axis = 1)
    y = dataset['score']
    test_size = 0.2
    X_train, X_test, y_train, y_test = train_test_split(X, y,random_state = 1, 
                                                        test_size = test_size)
    
    reg = ensemble.GradientBoostingRegressor()
    model_name = 'GradientBoostingRegressor'
    regs = get_modes_result(model_name, reg, 
                            X, y, X_train, y_train, X_test, y_test)
    
    parameter_path = data_folder + '\\preprocess_parameter.pkl'
    joblib.dump(preprocess_parameter, parameter_path)    
    
    model_path = data_folder + '\\%s.pkl'%model_name
    joblib.dump(regs, model_path) 
#    regs = joblib.load('filename.pkl') 


#%%
