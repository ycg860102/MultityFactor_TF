# -*- coding: utf-8 -*-

"""
Created on Wed Aug 29 09:07:41 2018

@author: yangchg
"""
from __future__ import division
import pandas as pd
import numpy as np

import sys
sys.path.append("D:\Program Files\Tinysoft\Analyse.NET") 
reload(sys)    
sys.setdefaultencoding('utf8')
import TSLPy2

def halflife(half_life=63,length=252):
    t = np.arange(length)
    w = 2**(t/half_life) / np.sum(2 ** (t/half_life))
    return w


def winsorize(factor):
    factor= factor.copy()
    factorMedia = factor.median() 
    MAD = (factor-factorMedia).apply(abs).median()   
    factor.loc[factor>(factorMedia+3*1.4826*MAD)]= factorMedia+3*1.4826*MAD
    factor.loc[factor<(factorMedia-3*1.4826*MAD)]= factorMedia-3*1.4826*MAD
    return factor

def normalize(factor):
    #zscore标准化s
    factorMean = factor.mean()
    factorStd = factor.std()
    factor = factor.apply(lambda x : (x-factorMean)/factorStd if factorStd<>0 else (x-factorMean))
    return factor   

def getBK(bk,adjustDay):
    #按照调仓日期取板块信息，天软函数getbkByName，会剔除调仓日一字涨跌停、停牌以及上市时间小于120日的股票
    BKStocks = TSLPy2.RemoteCallFunc('getbkByName2',[bk,TSLPy2.EncodeDate(int(adjustDay[:4]),int(adjustDay[5:7]),int(adjustDay[8:10]))],{}) 
    BKStocks = pd.DataFrame(BKStocks[1])
    BKStocks["SWNAME"] = BKStocks["SWNAME"].apply(lambda x : x.decode('gbk')) 
    BKStocks["stock_code"] = BKStocks["id"].apply(lambda x :x[2:]) 
    BKStocks["TotalValue"] = BKStocks["TotalValue"].apply(np.log)
    return BKStocks 

def getAdjustPeriods(begd,endd,freq):
    #设置调仓周期为月度调仓
    adjustPeriods = TSLPy2.RemoteCallFunc('getAdjustPeriod',[begd,endd,freq],{})
    adjustPeriods = pd.DataFrame(adjustPeriods[1])
    adjustPeriods["nextAdjustDay"] = adjustPeriods["date"].shift(-1)
    return adjustPeriods
        
def getFactorData(bk,adjustPeriods,FactorName,factorsInfo):
    tableName = factorsInfo.get("tableName")
    #direction = factorsInfo.get("direction") #因子方向1为正序，0位逆序
    #reciprocal = factorsInfo.get("reciprocal") #因子值是否取倒数
    #isLogDeal = factorsInfo.get("isLogDeal") #因子是否进行ln处理
    
    factorsResult =[]
    with pd.HDFStore("FactorDatas/"+tableName+'.h5',"r",complevel=9) as store:
        for i in adjustPeriods.index[:-1]:
            adjustDay = adjustPeriods.ix[i,"date"]
            #nextAdjustDay = adjustPeriods.ix[i,"nextAdjustDay"] 
            factor= store.select('/'+tableName+'/'+FactorName,where=["Date='{date}'".format(date=adjustDay.replace('-',''))])
            factor.drop_duplicates(inplace=True)
            #将日期字段设置为日期类型
            factor.con_date = pd.to_datetime(factor.Date) 
            factor['stockID']=factor["stockID"].apply(lambda x :x[2:]) 
            #factor.set_index(['Date','stockID'],inplace=False)
            factorsResult.append(factor)
    #合并各期数据   
    factorsResult = pd.concat(factorsResult,ignore_index=True)  
    
def getFactorDataOnePeriod(adjustDay,FactorName,factorsInfo):
    tableName = factorsInfo.get("tableName")
    #direction = factorsInfo.get("direction") #因子方向1为正序，0位逆序
    #reciprocal = factorsInfo.get("reciprocal") #因子值是否取倒数
    #isLogDeal = factorsInfo.get("isLogDeal") #因子是否进行ln处理

    with pd.HDFStore("FactorDatas/"+tableName+'.h5',"r",complevel=9) as store:
        #nextAdjustDay = adjustPeriods.ix[i,"nextAdjustDay"] 
        factor= store.select('/'+tableName+'/'+FactorName,where=["Date='{date}'".format(date=adjustDay.replace('-',''))])
        factor.drop_duplicates(inplace=True)
        #将日期字段设置为日期类型
        factor["con_date"] = pd.to_datetime(factor.Date) 
        factor['stock_code']=factor["stockID"].apply(lambda x :x[2:]) 
        #factor.set_index(['Date','stockID'],inplace=False)
    return factor
    
def getStockListPriceTSL(Stocklist,begDay,endDay,args) :
    prices= TSLPy2.RemoteCallFunc('getStocksPrice',[Stocklist,begDay,endDay,args],{})   
    prices = prices[1]
    return pd.DataFrame(prices).T 
    