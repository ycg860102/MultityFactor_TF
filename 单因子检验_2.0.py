# -*- coding: utf-8 -*-
"""
Created on Tue Jul 03 15:15:13 2018

计算逻辑：
    1、获取调仓期日期序列，处理各调仓期间股票涨跌情况，调仓日股票行业分类；
    2、按照调仓日统一处理各因子值数据，并统一放置在同一个DataFrame中。
    3、采用RLM方法计算各因子各截面数据的回归信息

@author: yangchg
"""
import pandas as pd  
from sqlalchemy import create_engine 
import numpy as np 
import matplotlib.pyplot as plt 
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
import utility 
import logging 

logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)
handler = logging.FileHandler("log.txt")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

console = logging.StreamHandler()
console.setLevel(logging.INFO)

logger.addHandler(handler)
logger.addHandler(console)


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
    
    #去极值
    #factorsResult[FactorName] = utility.winsorize(factorsResult[FactorName])
    #factorsResult[FactorName] = factorsResult.groupby("Date")[FactorName].apply(utility.winsorize)
    #标准化
    #factorsResult[FactorName] = utility.normalize(factorsResult[FactorName])
    #factorsResult[FactorName] = factorsResult.groupby("Date")[FactorName].apply(utility.normalize)
    #返回因子数据
    return factorsResult.set_index(['Date','stockID'],inplace=False)
        
    
def getStockZF(adjustPeriods):
    #取得股票区间收益值
    stockZF = []
    with pd.HDFStore('FactorDatas/StockZF.h5',"r",complevel=9) as store:
        for i in adjustPeriods.index[:-1]:
            adjustDay = adjustPeriods.ix[i,"date"]
            nextAdjustDay = adjustPeriods.ix[i,"nextAdjustDay"] 
            factor= store.select('/StockZF/stockZF',where=["Date>'{date}'".format(date=adjustDay.replace('-','')),
                                                           "Date<='{date}'".format(date=nextAdjustDay.replace('-',''))])
            factor.drop_duplicates(inplace=True)        
            factor['stockID']=factor["stockID"].apply(lambda x :x[2:]) 
            factor['stockZF'] = factor['stockZF']/100+1 
            stockZF.append(factor.groupby(['stockID'])['stockZF'].apply(np.prod).rename(adjustDay.replace('-',''))) 
    stockZF = pd.DataFrame(stockZF).stack().rename('stockZF',inplace=True)
    return stockZF
    
def regresstion(factor,FactorName):
    y=factor[FactorName]
    X=pd.get_dummies(factor['SWNAME'])
    if FactorName<>'CAP':
        X['TotalValue'] = factor['TotalValue']
    X=sm.add_constant(X)
    
    #res = sm.OLS(y, X).fit() #通过OLS进行回归
    res2= sm.RLM(y, X).fit()  #通过RLM进行回归
    #res3= sm.WLS(y, X).fit() #通过WLS进行回归
    
    #tinyedFactor列，为回归后的残差项，看做新的因子值
    resid = res2.resid
    return resid.rename(FactorName)
    
def getT_IC_RANKIC(regFactor,stockzf):
    #计算T值和P值
    factorT,factorP=ttest_rel(regFactor,stockzf) 
    #计算IC值和RANKIC值
    IC = stockzf.corr(regFactor)
    rankIC = stockzf.corr(regFactor,method="spearman")
    
    return {"factorT":factorT,"factorP":factorP,"IC":IC,"rankIC":rankIC}


if __name__ == '__main__':
        
    #获取调仓周期，周期分为月度和周度可以选择
    begd=TSLPy2.EncodeDate(2017,7,1)
    endd=TSLPy2.EncodeDate(2018,9,7)
    adjustPeriods = getAdjustPeriods(begd,endd,u"月线")
    
    #板块名称
    bk = u"A股" 
    bk = u"申万计算机" 
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
    #CAP = getFactorData(bk,adjustPeriods,"CAP",{"tableName":"CAP","direction":1,"reciprocal":0,"isLogDeal":0}) 
    
    #获取各调仓日的因子值 
    factorList = []
    for FactorName,factorsInfo in factors.iteritems():
        factorList.append(getFactorData(bk,adjustPeriods,FactorName,factorsInfo))
    factorDatas = pd.concat(factorList,axis=1) 

    #取得股票区间收益值
    stockZF = getStockZF(adjustPeriods) 
    
    #依次循环每个截面
    FactorT_IC_RankIC=[]
    for i in adjustPeriods.index[:-1]:
        adjustDay = adjustPeriods.ix[i,"date"]
        nextAdjustDay = adjustPeriods.ix[i,"nextAdjustDay"]
        logger.info(u"开始处理"+adjustDay+u"天数据！")
        #如果调仓日期大于因子中的最大日期，则调出循环
        if int(adjustDay.replace('-','')) > int(factorDatas.index.levels[0].max()):
            break 
        #获取个截面上的板块数据，并剔除截面上未上市，涨停以及停牌股票
        BKStocks = utility.getBK(bk,adjustDay)
        factor = factorDatas.loc[(adjustDay.replace('-',''),slice(None)),:]
        factor.reset_index(level='stockID',inplace=True)
        factor = factor.merge(BKStocks,left_on='stockID',right_on='stock_code',how='right')
        factor.set_index("stock_code",inplace=True) 
           
        factor[factors.keys()]=factor[factors.keys()].apply(utility.winsorize).apply(utility.normalize)
        
        #在各截面上对各因子进行回归，剔除行业和市值影响
        regressedFactors = []
        for FactorName,factorsInfo in factors.iteritems():
            regFactor = regresstion(factor,FactorName)
            regressedFactors.append(regFactor)
            _T_IC_RankIC = getT_IC_RANKIC(regFactor,(stockZF[adjustDay.replace('-','')].reindex(regFactor.index)-1).fillna(0))
            _T_IC_RankIC["adjustDay"]= adjustDay.replace('-','')
            _T_IC_RankIC["FactorName"]= FactorName
            FactorT_IC_RankIC.append(_T_IC_RankIC)
        regressedFactors = pd.concat(regressedFactors,axis=1) 

    FactorT_IC_RankIC=pd.DataFrame(FactorT_IC_RankIC)    
    
    FactorT_IC_RankIC.groupby("FactorName").mean().sort_values(by='rankIC',ascending=False)
    FactorT_IC_RankIC.to_excel("FactorT_IC_RankIC.xlsx")
    
    resultICMean = FactorT_IC_RankIC.groupby(["FactorName"]).mean().sort_values(by='IC',ascending=False)
    resultSL = FactorT_IC_RankIC[FactorT_IC_RankIC["IC"]>0].groupby(["FactorName"])["IC"].count() / FactorT_IC_RankIC.groupby(["FactorName"])["IC"].count()
    resultICIR = FactorT_IC_RankIC.groupby(["FactorName"])["IC","rankIC"].apply(lambda x : x.mean()/x.std()).sort_values(by='IC')
    
    for FactorName,factorsInfo in factors.iteritems():
        FactorT_IC_RankIC[FactorT_IC_RankIC["FactorName"]==FactorName].set_index("adjustDay")[["IC","rankIC"]].plot(kind="bar",title=FactorName) 
        plt.savefig("pic/"+FactorName+'.png') 
        plt.close() 
    
    