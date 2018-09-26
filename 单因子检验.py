# -*- coding: utf-8 -*-
"""
Created on Tue Jul 03 15:15:13 2018

计算逻辑：
    1、根据调仓期，在每个调仓期初获取因子值，并根据因子值大小排序，并分为5组。
    2、取每个调仓期间的个股行情价格，并根据每日涨跌幅，计算每个股票的当日净值，按照分组计算每组净值。
    3、根据每组净值情况，计算每组的净值涨跌幅。
    4、将所有调仓期的净值数据合并，按照分组计算净值。

@author: yangchg
"""
import pandas as pd  
from sqlalchemy import create_engine 
import numpy as np 
#import matplotlib.pyplot as plt 
#import matplotlib.dates as mdate
import datetime,os
#import seaborn as sns 
import statsmodels.api as sm
from patsy import dmatrices
from scipy.stats import ttest_rel

import sys
sys.path.append("D:\Program Files\Tinysoft\Analyse.NET") 
reload(sys)    
sys.setdefaultencoding('utf8')
import TSLPy2

import logging 

def dealData(bk,begd,endd,adjustPeriods,factorsInfo,FactorName,Path):
    #数据库连接引擎
    
    tableName = factorsInfo.get("tableName")
    direction = factorsInfo.get("direction") #因子方向1为正序，0位逆序
    reciprocal = factorsInfo.get("reciprocal") #因子值是否取倒数
    isLogDeal = factorsInfo.get("isLogDeal") #因子是否进行ln处理
    
    #engine = create_engine('mysql://root:root@172.16.158.142/dwlh?charset=utf8') 
    
    store = pd.HDFStore(tableName+'.h5',"r",complevel=9) 
        
    periedValues = []
    #循环每次调仓日期
    for i in adjustPeriods.index[:-1]:
        
        adjustDay = adjustPeriods.ix[i,"date"]
        nextAdjustDay = adjustPeriods.ix[i,"nextAdjustDay"] 
        logging.warning(u"处理第"+adjustDay+u"天数据!")
        factor= store.select('/'+tableName+'/'+FactorName,where=["Date='{date}'".format(date=adjustDay.replace('-',''))])
        
        #将日期字段设置为日期类型
        factor.con_date = pd.to_datetime(factor.Date) 
        factor['stock_code']=factor["stockID"].apply(lambda x :x[2:]) 
        
        #按照调仓日期取板块信息，天软函数getbkByName，会剔除调仓日一字涨跌停、停牌以及上市时间小于120日的股票
        BKStocks = TSLPy2.RemoteCallFunc('getbkByName2',[bk,TSLPy2.EncodeDate(int(adjustDay[:4]),int(adjustDay[5:7]),int(adjustDay[8:10]))],{}) 
        BKStocks = pd.DataFrame(BKStocks[1])
        BKStocks["SWNAME"] = BKStocks["SWNAME"].apply(lambda x : x.decode('gbk')) 
        BKStocks["stock_code"] = BKStocks["id"].apply(lambda x :x[2:]) 
        BKStocks["TotalValue"] = BKStocks["TotalValue"].apply(np.log) 
        #对因子值和板块合并
        factor = factor.merge(BKStocks,on="stock_code")
        #判断是否对因子值进行倒序处理
        if reciprocal ==1 : 
            factor[FactorName]=factor[FactorName].apply(lambda x : 1/x  if x<>0 else x ) 
            
        if isLogDeal ==1 :
            factor[FactorName]=factor[FactorName].apply(np.log) 
        
        #对因子值进行方向处理
        factor[FactorName]=factor[FactorName]*direction  
        
        #替换异常值
        factorMedia = factor[FactorName].median() 
        MAD =  (factor[FactorName]-factorMedia).apply(abs).median()   
        factor.loc[factor[FactorName]>(factorMedia+3*1.4826*MAD),FactorName]= factorMedia+3*1.4826*MAD
        factor.loc[factor[FactorName]<(factorMedia-3*1.4826*MAD),FactorName]= factorMedia-3*1.4826*MAD
        
        #zscore标准化
        factorMean = factor[FactorName].mean()
        factorStd = factor[FactorName].std()
        factor[FactorName] = factor[FactorName].apply(lambda x : (x-factorMean)/factorStd if factorStd<>0 else (x-factorMean))
        
        #下期收益序列：
        stokzf = pd.DataFrame(TSLPy2.RemoteCallFunc('getStockZF',[bk,TSLPy2.EncodeDate(int(adjustDay[:4]),int(adjustDay[5:7]),int(adjustDay[8:10])),
                                                                  TSLPy2.EncodeDate(int(nextAdjustDay[:4]),int(nextAdjustDay[5:7]),int(nextAdjustDay[8:10]))],{})[1])
        factor= factor.merge(stokzf,on="stock_code") 
        factor.set_index("stock_code",inplace=True)
        
        #通过回归对因子进行行业和市值中性化处理
        factor = factor.dropna()
        #方法1
        #y, X = dmatrices('{factorName} ~  SWNAME + TotalValue'.format(factorName=FactorName), data=factor, return_type='dataframe')
        #方法2
        y=factor[FactorName]
        X=pd.get_dummies(factor['SWNAME'])
        if FactorName<>'CAP':
            X['TotalValue'] = factor['TotalValue']
        X=sm.add_constant(X)
        
        #res = sm.OLS(y, X).fit() #通过OLS进行回归
        res2= sm.RLM(y, X).fit()  #通过RLM进行回归
        #res3= sm.WLS(y, X).fit() #通过WLS进行回归
        #factorParam = res2.params[FactorName]
        #factorT = res2.tvalues[FactorName]
        
        #tinyedFactor列，为回归后的残差项，看做新的因子值
        factor["tinyedFactor2"] = factor[FactorName]- res2.fittedvalues
        factor["tinyedFactor"] =  res2.resid
        #对新的因子值和下期涨幅进行T检验，得到T值和P值
        factorT,factorP=ttest_rel(factor["tinyedFactor"],factor["zf"]) 
        
         #计算IC值和RANKIC值
        IC = factor["zf"].corr(factor["tinyedFactor"])
        rankIC = factor["zf"].corr(factor["tinyedFactor"],method="spearman")
        
        periedValues.append(pd.DataFrame({"FactorName":FactorName,
                      "adjustDay":adjustDay,
                      "IC":IC,
                      "rankIC":rankIC,
                      "factorP":factorP,
                      "factorT":factorT
                      },index=[0]))
        
        """
        fig, ax = plt.subplots(figsize=(8,6))

        ax.plot(factor["con_roe"], y, 'o', label="Data")
        #ax.plot(x["con_roe"], y_true, 'b-', label="True")
        ax.plot(factor["con_roe"], res2.fittedvalues, 'r--.', label="RLMPredicted")
        ax.plot(factor["con_roe"], res.fittedvalues, 'b--.', label="OLSPredicted")
        legend = ax.legend(loc="best")
        """
    store.close()    
    return periedValues
        
    
if __name__ == '__main__':
    
    #获取调仓周期，周期分为月度和周度可以选择
    begd=TSLPy2.EncodeDate(2017,8,1)
    endd=TSLPy2.EncodeDate(2018,8,28)
    #设置调仓周期为月度调仓
    adjustPeriods = TSLPy2.RemoteCallFunc('getAdjustPeriod',[begd,endd,u"月线"],{})
    adjustPeriods = pd.DataFrame(adjustPeriods[1])
    adjustPeriods["nextAdjustDay"] = adjustPeriods["date"].shift(-1)
    #在当前目录下新增路径，如没有文件夹则新增文件夹
    Path = u"单因子检验\\"+datetime.datetime.now().strftime("%Y-%m-%d")
    if os.path.exists(Path)  and os.access(Path, os.R_OK):
        print Path , ' is exist!' 
    else:
        os.makedirs(Path) 
    #板块名称
    bk = u"A股" 
    #bk = u"申万计算机" 
    factors = {"CAP":{"tableName":"CAP","direction":1,"reciprocal":0,"isLogDeal":0},
               "1MReverse":{"tableName":"Reverse","direction":1,"reciprocal":0},
               "3MReverse":{"tableName":"Reverse","direction":-1,"reciprocal":0},
               "ATR":{"tableName":"ATR","direction":-1,"reciprocal":0},
               "1MTurnOver":{"tableName":"Float","direction":-1,"reciprocal":0},
               "TURNOVERRATE":{"tableName":"Float","direction":-1,"reciprocal":0},
               "ILLIQ":{"tableName":"Float","direction":-1,"reciprocal":0},
               "BP":{"tableName":"Value","direction":-1,"reciprocal":0},
               "EP":{"tableName":"Value","direction":-1,"reciprocal":0},
               "CFP":{"tableName":"Value","direction":-1,"reciprocal":0},
               "SP":{"tableName":"Value","direction":-1,"reciprocal":0},
               "EP_TTM":{"tableName":"Value","direction":-1,"reciprocal":0},
               "QPROFITYOY":{"tableName":"Grouth","direction":-1,"reciprocal":0},
               "QSALESYOY":{"tableName":"Grouth","direction":-1,"reciprocal":0},
               "ROE":{"tableName":"Profit","direction":-1,"reciprocal":0},
               "ROIC":{"tableName":"Profit","direction":-1,"reciprocal":0},
               "GPM":{"tableName":"Profit","direction":-1,"reciprocal":0},
               }

    finalbx = []
    for FactorName,factorsInfo in factors.iteritems():
        finalbx += dealData(bk,begd,endd,adjustPeriods,factorsInfo,FactorName,Path) 
    result = pd.concat(finalbx,ignore_index=True) 
    
    resultICMean = result.groupby(["FactorName"]).mean().sort_values(by='IC',ascending=False)
    resultSL = result[result["IC"]>0].groupby(["FactorName"])["IC"].count() / result.groupby(["FactorName"])["IC"].count()
    resultICIR = result.groupby(["FactorName"])["IC","rankIC"].apply(lambda x : x.mean()/x.std()).sort_values(by='IC')
    #pd.concat(finalbx).to_excel(Path+"\\"+bk+u"板块所有因子表现.xlsx") 
    

        