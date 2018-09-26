# -*- coding: utf-8 -*-
"""
Created on Fri Aug 10 14:12:35 2018

@author: yangchg


从天软中获取因子值数据，并将数据存储为HDF5结构

"""

import pandas as pd 
import numpy as np
import datetime 
from sqlalchemy import create_engine 

import sys
sys.path.append("D:\Program Files\Tinysoft\Analyse.NET") 
reload(sys)    
sys.setdefaultencoding('utf8')
import TSLPy2


"""
#从天软中获取股票市值，
输入参数：板块名称，默认为全A
         开始时间：类型为int，默认为当日
         结束时间：类型为int，默认为当日
返回数据:二维表，其中第一个字段为日期，第二个字段为股票代码，第三个字段为市值，单位万元 
"""

def getFactorAndToDataBase(bk,begMonth,endMonth,tableName):
    
    months = pd.period_range(begMonth,endMonth,freq='M') 
    now = datetime.datetime.now().strftime("%Y%m%d")
    
    for month in months :
        begDay = month.asfreq('B',how='begin').strftime("%Y%m%d")
        endDay = month.asfreq('B',how='end').strftime("%Y%m%d")
    
        if now < endDay :
            endDay = now
        #输入板块，开始日期，截止日期，板块名称，将从天软中提取数据，并写入mysql数据库    
        getFactorData(bk,begDay,endDay,tableName) 
        

def getFactorData(bk,begDay,endDay,tableName):      
    FactorDatas = TSLPy2.RemoteCallFunc('getFactorData',[bk,int(begDay),int(endDay),tableName],{}) 
    FactorDatas = pd.DataFrame(FactorDatas[1]) 
    #写入数据库
    #FactorDatas.to_sql(factorName,engine,if_exists='append',index=False) 
    
    #写入HDF5文件
    with pd.HDFStore("FactorDatas/"+tableName+'.h5',"a",complevel=9) as store :

        columns = list(FactorDatas.columns)
        [columns.remove(x)  for x  in ['Date','stockID']]
        for col in columns : 
            store.append(tableName+'/'+col,FactorDatas[['Date','stockID',col]],data_columns=['Date','stockID'])
    #return FactorDatas   

def getFactorData_new(bk,begDay,endDay,tableName,factorByName):      
    FactorDatas = TSLPy2.RemoteCallFunc('getFactorData',[bk,int(begDay),int(endDay),tableName],{}) 
    FactorDatas = pd.DataFrame(FactorDatas[1])
    if len(FactorDatas.index)==0 :
        return None
    #写入数据库
    #FactorDatas.to_sql(factorName,engine,if_exists='append',index=False) 
    
    #写入HDF5文件
    with pd.HDFStore("FactorDatas/"+tableName+'.h5',"a",complevel=9) as store :

        columns = list(FactorDatas.columns)
        [columns.remove(x)  for x  in ['Date','stockID']]
        for col in columns : 
            factorDate = factorByName.loc[col,"LastDay"]
            _FactorDada = FactorDatas[FactorDatas['Date']>factorDate][['Date','stockID',col]]
            store.append(tableName+'/'+col,_FactorDada,data_columns=['Date','stockID'])
    #return FactorDatas 
      
        
def getFactorMaxDay(factorName,tableName) : 
    with pd.HDFStore("FactorDatas/"+tableName+'.h5',"r",complevel=9) as store : 
        return store.select_column("/"+tableName+'/'+factorName,'Date').max() 
    
bk = u'a股'
engine = create_engine('mysql://root:root@127.0.0.1/dwlh?charset=utf8')#用sqlalchemy创建引擎

"""
#以下为初始化各因子数据，并写入h5文件中
获取市值因子数据 ,并写入数据库
CAPFactorDatas = getFactorAndToDataBase(bk,'201001','201808','CAP')  
    
获取反转因子数据 ,并写入数据库
ReverseFactorDatas = getFactorAndToDataBase(bk,'201001','201808','Reverse')   

获取波动率因子数据 ,并写入数据库
ATRFactorDatas = getFactorAndToDataBase(bk,'201001','201808','ATR')     
    
获取流动性因子数据 ,并写入数据库
FloatFactorDatas = getFactorAndToDataBase(bk,'201001','201808','Float') 

获取估值因子数据 ,并写入数据库
ValueFactorDatas = getFactorAndToDataBase(bk,'201001','201808','Value') 

获取成长因子数据 ,并写入数据库
GrouthFactorDatas = getFactorAndToDataBase(bk,'201001','201808','Grouth') 

获取盈利因子数据 ,并写入数据库
ProfitFactorDatas = getFactorAndToDataBase(bk,'201001','201808','Profit') 

获取股票涨跌幅 ,并写入数据库
ProfitFactorDatas = getFactorAndToDataBase(bk,'201001','201808','StockZF')
"""


factors = pd.read_excel('factorSetting.xlsx') 
factors["factorName"] = factors.index 
factors["LastDay"] = factors.apply(lambda x : getFactorMaxDay(x.factorName,x.tableName),axis=1) 
tableNames=factors.tableName.unique() 

for tableName in tableNames: 
    
    factorByName = factors[factors['tableName']==tableName] 
    
    factorMinDay = factorByName["LastDay"].min() 
        
    now = datetime.datetime.now().strftime("%Y%m%d") 
    day = datetime.timedelta(days=1) 
    begDay = datetime.datetime.strptime(factorMinDay,'%Y%m%d')+day 
    begDay = begDay.strftime('%Y%m%d') 
    getFactorData_new(bk,begDay,'20180917',tableName,factorByName) 
    
    #getFactorData(,factorName)

