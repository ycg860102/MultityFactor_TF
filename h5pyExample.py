# -*- coding: utf-8 -*-
"""
Created on Mon Aug 20 08:29:32 2018

@author: yangchg
"""

import sys
import h5py ,datetime
import numpy as np
import pandas as pd


sys.path.append("D:\Program Files\Tinysoft\Analyse.NET") 
reload(sys)    
sys.setdefaultencoding('utf8')
import TSLPy2

def save_h5(times=0):
    if times == 0:
        h5f = h5py.File('1MTurnOver.h5', 'w')
        datasetRQ = h5f.create_dataset(u"日期", (30,),
                                     maxshape=(None,),
                                     # chunks=(1, 1000, 1000),
                                     dtype='float32')
        datasetID = h5f.create_dataset(u"ID", (30,),
                                     maxshape=(None,),
                                     # chunks=(1, 1000, 1000),
                                     dtype='float32')
        dataset = h5f.create_dataset("data", (100, 1000, 1000),
                                     maxshape=(None, 1000, 1000),
                                     # chunks=(1, 1000, 1000),
                                     dtype='float32')
    else:
        h5f = h5py.File('1MTurnOver.h5', 'a')
        dataset = h5f['data']
    # 关键：这里的h5f与dataset并不包含真正的数据，
    # 只是包含了数据的相关信息，不会占据内存空间
    #
    # 仅当使用数组索引操作（eg. dataset[0:10]）
    # 或类方法.value（eg. dataset.value() or dataset.[()]）时数据被读入内存中
    a = np.random.rand(100, 1000, 1000).astype('float32')
    # 调整数据预留存储空间（可以一次性调大些）
    dataset.resize([times*100+100, 1000, 1000])
    # 数据被读入内存 
    dataset[times*100:times*100+100] = a
    # print(sys.getsizeof(h5f))
    h5f.close()

def load_h5():
    h5f = h5py.File('data.h5', 'r')
    data = h5f['data'][0:10]
    print(data)


def getFactorAndToDataBase(bk,begMonth,endMonth,factorName):
    
    months = pd.period_range(begMonth,endMonth,freq='M') 
    now = datetime.datetime.now().strftime("%Y%m%d")
    
    for month in months :
        begDay = month.asfreq('B',how='begin').strftime("%Y%m%d")
        endDay = month.asfreq('B',how='end').strftime("%Y%m%d")
    
        if now < endDay :
            endDay = now
        #输入板块，开始日期，截止日期，板块名称，将从天软中提取数据，并写入mysql数据库    
        getFactorData(bk,begDay,endDay,factorName) 
        

def getFactorData(bk,begDay,endDay,factorName):      
    FactorDatas = TSLPy2.RemoteCallFunc('getFactorData',[bk,int(begDay),int(endDay),factorName],{}) 
    FactorDatas = pd.DataFrame(FactorDatas[1])
    #FactorDatas.to_sql(factorName,engine,if_exists='append',index=False)
    #return FactorDatas 

if __name__ == '__main__':
    # save_h5(0)
    for i in range(5):
        save_h5(i)
    # 部分数据导入内存
    #load_h5()