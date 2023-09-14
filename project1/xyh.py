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

chepai='皖PV789V'
sql1="select * from yh where HPHM = '%s'"%chepai
yh1=pd.read_sql(sql1,conn)

def time2stamp(cmnttime):
    cmnttime=datetime.strptime(cmnttime,"%Y-%m-%d %H:%M:%S.%f")
    stamp=int(datetime.timestamp(cmnttime))
    return stamp

yh1['JGSJ']=yh1['JGSJ'].apply(time2stamp)
yh1.sort_values(by='JGSJ',inplace=True,ascending=True)
yh1=yh1.reset_index(drop=True)

yh1_suoyin=yh1.index.tolist()
print(yh1)
data=pd.DataFrame(columns=['HPHM','92to107','107to93','93to107','107to92','jtime','flag'])

for i in range(len(yh1_suoyin)-1):
    westflag=0
    eastflag=0
    jtime=0
    if yh1.loc[i,'SSID']=='HK-92' and (yh1.loc[i,'CDBH']=='7' or yh1.loc[i,'CDBH']=='9'):
        if yh1.loc[i+1,'SSID']=='HK-107' and (yh1.loc[i+1,'CDBH']=='3' or yh1.loc[i+1,'CDBH']=='4'):   
            time1=yh1.loc[i+1,'JGSJ']
            time2=yh1.loc[i,'JGSJ']
            jtime=time1-time2
            data.loc[i,'HPHM']='%s'%chepai
            data.loc[i,'92to107']=1
            data.loc[i,'jtime']=jtime
            data.loc[i,'flag']=1
            #print(jtime)
    if yh1.loc[i,'SSID']=='HK-107' and (yh1.loc[i,'CDBH']=='3' or yh1.loc[i,'CDBH']=='4'):
        if yh1.loc[i+1,'SSID']=='HK-93' and (yh1.loc[i+1,'CDBH']=='7' or yh1.loc[i+1,'CDBH']=='8' or yh1.loc[i+1,'CDBH']=='9'):
            time1=yh1.loc[i+1,'JGSJ']
            time2=yh1.loc[i,'JGSJ']
            jtime=time1-time2
            data.loc[i,'HPHM']='%s'%chepai
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
            data.loc[i,'HPHM']='%s'%chepai
            data.loc[i,'93to107']=1
            data.loc[i,'jtime']=jtime
            data.loc[i,'flag']=1
            #print(jtime)
    if yh1.loc[i,'SSID']=='HK-107' and (yh1.loc[i,'CDBH']=='1' or yh1.loc[i,'CDBH']=='2'):
        if yh1.loc[i+1,'SSID']=='HK-92' and (yh1.loc[i+1,'CDBH']=='1' or yh1.loc[i+1,'CDBH']=='2' or yh1.loc[i+1,'CDBH']=='3'):
            time1=yh1.loc[i+1,'JGSJ']
            time2=yh1.loc[i,'JGSJ']
            jtime=time1-time2
            data.loc[i,'HPHM']='%s'%chepai
            data.loc[i,'107to92']=1
            data.loc[i,'jtime']=jtime
            data.loc[i,'flag']=1
            eastflag=1
            #print(jtime)
    if westflag==1:
        x=i-1
        if x>=0:
            if yh1.loc[x,'SSID']=='HK-92' and (yh1.loc[x,'CDBH']=='7' or yh1.loc[x,'CDBH']=='9'):
                jtime=data.loc[i,'jtime']+data.loc[x,'jtime']
                data.loc[i,'jtime']=jtime
                data.loc[x,'flag']=0
    if eastflag==1:
        x=i-1
        if x>=0:
            if yh1.loc[x,'SSID']=='HK-93' and (yh1.loc[i,'CDBH']=='2' or yh1.loc[i,'CDBH']=='3' or yh1.loc[i,'CDBH']=='4' or yh1.loc[i,'CDBH']=='12'):
                jtime=data.loc[i,'jtime']+data.loc[x,'jtime']
                data.loc[x,'flag']=0
                data.loc[i,'jtime']=jtime

data_suoyin=data.index.tolist()
for i in range(len(data_suoyin)):
    if data.loc[i,'flag']==1:
        print(data.loc[i,'jtime'])


print(data)
















