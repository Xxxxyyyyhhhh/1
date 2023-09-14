import numpy as np            
import pandas as pd
import pymysql
from datetime import datetime,date

conn = pymysql.connect(
    host='localhost',
    port=3306,
    user='root',
    password='123456',
    database='project1',
)

cursor=conn.cursor()
sql1="select HPHM from yh"
chepai=pd.read_sql(sql1,conn)
chepai.drop_duplicates(subset='HPHM',inplace=True,ignore_index=True)
chepai_list=list(chepai['HPHM'])
chepai_list1=chepai_list[:len(chepai_list)//2]
chepai_list2=chepai_list[len(chepai_list)//2:]
#chepai_list1=chepai_list[:3]

TGSJ=pd.DataFrame(columns=['HPHM','TGSJ'])

def car(chepai1):
    sql2="select * from yh where HPHM = '%s'"%chepai1
    data1=pd.read_sql(sql2,conn)
    return data1

def time2stamp(cmnttime):
    cmnttime=datetime.strptime(cmnttime,"%Y-%m-%d %H:%M:%S.%f")
    stamp=int(datetime.timestamp(cmnttime))
    return stamp

def paixu(samecar):
    samecar['JGSJ']=samecar['JGSJ'].apply(time2stamp)
    samecar.sort_values(by='JGSJ',inplace=True,ascending=True)
    samecar=samecar.reset_index(drop=True)
    return samecar

def jisuan(yh1):
    data=pd.DataFrame(columns=['HPHM','92to107','107to93','93to107','107to92','jtime','flag'])
    data=data.drop(index=data.index,)
    for i in range(len(yh1_suoyin)-1):
        westflag=0
        eastflag=0
        jtime=0
        if yh1.loc[i,'SSID']=='HK-92' and (yh1.loc[i,'CDBH']=='7' or yh1.loc[i,'CDBH']=='9'):
            if yh1.loc[i+1,'SSID']=='HK-107' and (yh1.loc[i+1,'CDBH']=='3' or yh1.loc[i+1,'CDBH']=='4'):   
                time1=yh1.loc[i+1,'JGSJ']
                time2=yh1.loc[i,'JGSJ']
                jtime=time1-time2
                data.loc[i,'HPHM']='%s'%chepai1
                data.loc[i,'92to107']=1
                data.loc[i,'jtime']=jtime
                data.loc[i,'flag']=1
                #print(jtime)
        if yh1.loc[i,'SSID']=='HK-107' and (yh1.loc[i,'CDBH']=='3' or yh1.loc[i,'CDBH']=='4'):
            if yh1.loc[i+1,'SSID']=='HK-93' and (yh1.loc[i+1,'CDBH']=='7' or yh1.loc[i+1,'CDBH']=='8' or yh1.loc[i+1,'CDBH']=='9'):
                time1=yh1.loc[i+1,'JGSJ']
                time2=yh1.loc[i,'JGSJ']
                jtime=time1-time2
                data.loc[i,'HPHM']='%s'%chepai1
                data.loc[i,'107to93']=1
                data.loc[i,'jtime']=jtime
                data.loc[i,'flag']=1
                westflag=1
                #print(jtime)        
        if yh1.loc[i,'SSID']=='HK-93' and (yh1.loc[i,'CDBH']=='2' or yh1.loc[i,'CDBH']=='3' or yh1.loc[i,'CDBH']=='4' or yh1.loc[i,'CDBH']=='12'):
            if yh1.loc[i+1,'SSID']=='HK-107' and (yh1.loc[i+1,'CDBH']=='1' or yh1.loc[i+1,'CDBH']=='2'):
                time1=yh1.loc[i+1,'JGSJ']
                time2=yh1.loc[i,'JGSJ']
                jtime=time1-time2
                data.loc[i,'HPHM']='%s'%chepai1
                data.loc[i,'93to107']=1
                data.loc[i,'jtime']=jtime
                data.loc[i,'flag']=1
                #print(jtime)
        if yh1.loc[i,'SSID']=='HK-107' and (yh1.loc[i,'CDBH']=='1' or yh1.loc[i,'CDBH']=='2'):
            if yh1.loc[i+1,'SSID']=='HK-92' and (yh1.loc[i+1,'CDBH']=='1' or yh1.loc[i+1,'CDBH']=='2' or yh1.loc[i+1,'CDBH']=='3'):
                time1=yh1.loc[i+1,'JGSJ']
                time2=yh1.loc[i,'JGSJ']
                jtime=time1-time2
                data.loc[i,'HPHM']='%s'%chepai1
                data.loc[i,'107to92']=1
                data.loc[i,'jtime']=jtime
                data.loc[i,'flag']=1
                eastflag=1
                #print(jtime)
        if westflag==1:
            x=i-1
            if x>=0:
                if yh1.loc[x,'SSID']=='HK-92' and (yh1.loc[x,'CDBH']=='7' or yh1.loc[x,'CDBH']=='9'):
                    time1=yh1.loc[i+1,'JGSJ']
                    time2=yh1.loc[i-1,'JGSJ']
                    jtime=time1-time2
                    data.loc[i,'jtime']=jtime
                    data.loc[x,'flag']=0
        if eastflag==1:
            x=i-1
            if x>=0:
                if yh1.loc[x,'SSID']=='HK-93' and (yh1.loc[i,'CDBH']=='2' or yh1.loc[i,'CDBH']=='3' or yh1.loc[i,'CDBH']=='4' or yh1.loc[i,'CDBH']=='12'):
                    time1=yh1.loc[i+1,'JGSJ']
                    time2=yh1.loc[i-1,'JGSJ']
                    jtime=time1-time2
                    data.loc[x,'flag']=0
                    data.loc[i,'jtime']=jtime
    return data

num=0

for i in range(len(chepai_list1)):
    chepai1=chepai_list1[i]
    #print(chepai1)
    samecar=car(chepai1)
    #print(samecar)
    yh1=paixu(samecar)
    yh1_suoyin=yh1.index.tolist()
    jieguo=jisuan(yh1)
    jieguo_suoyin=jieguo.index.tolist()
    #print(jieguo)
    jieguo=jieguo.reset_index(drop=True)
    for i in range(len(jieguo_suoyin)):
        if jieguo.loc[i,'flag']==1:
            TGSJ.loc[num,'HPHM']=jieguo.loc[i,'HPHM']
            TGSJ.loc[num,'TGSJ']=jieguo.loc[i,'jtime']
            num+=1
    jieguo=jieguo.drop(index=jieguo.index,)

print(TGSJ)    







