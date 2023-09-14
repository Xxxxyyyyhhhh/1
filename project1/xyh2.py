import cred
import pandas as pd
import pymysql
import time
from datetime import datetime,date

conn = pymysql.connect(
    host='localhost',
    port=3306,
    user='root',
    password='123456',
    database='project1',
)

cursor=conn.cursor()
sql="select * from yh"
cursor.execute(sql)
results=cursor.fetchall()
print(results)


#print(data)