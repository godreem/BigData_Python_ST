
# coding: utf-8

# # 미니프로젝트 - 계절성 지수 산출
# DB(ORACLE) => 분석 => DB(PostgreSQL)

# ## Library import

# In[2]:


import pandas as pd
import numpy as np
import pymysql
import cx_Oracle
# postgreSQL DB Lib
import psycopg2
from sqlalchemy import create_engine


# ## ORACLE DB에 TEST1 사용자로 접근 후 테이블 생성 및 데이터 IMPORT

# In[3]:


#create table kopo_channel_seasonality_new(
#REGIONID varchar(10),
#PRODUCT varchar(20),
#YEARWEEK varchar(6),
#QTY float
#)


# ## Oracle DB 커넥션 열기

# In[4]:


engine = create_engine('oracle+cx_oracle://test1:test1@127.0.0.1:1521/xe')


# ## 데이터 불러오기(파라미터 데이터)

# In[5]:


# Select table
seloutData = pd.read_sql_query('SELECT * FROM kopo_channel_seasonality_new', engine)


# In[6]:


seloutData.head()


# ## 컬럼을 대문자로 변경

# In[7]:


seloutData.columns = [x.upper() for x in seloutData.columns]
seloutData.head()


# ## 데이터 정제

# #### QTY 의 음수값을 0으로 변환

# In[8]:


seloutData["QTY_NEW"] = np.where(seloutData["QTY"]<1, 0,seloutData["QTY"])


# In[9]:


seloutData.head()


# #### 53주차 제거
# 53주차가 없는 상품이 있기 때문에 53주차 데이터를 제거

# In[10]:


seloutData = seloutData.loc[seloutData["YEARWEEK"].astype(str).str[4:6]!="53"]


# #### YEARWEEK 컬럼을 YEAR, WEEK로 분리
# 년도와 상관없이 주차별로 분석을 하기 위해 년도와 주차를 분리

# In[11]:


seloutData["YEAR"] = seloutData["YEARWEEK"].astype(str).str[0:4]
seloutData["WEEK"] = seloutData["YEARWEEK"].astype(str).str[4:6]


# ## 결과물을 저장할 테이블명 지정
# CHANNEL_RESULT

# ## 데이터 정렬

# In[12]:


seloutData.sort_values(["REGIONID","PRODUCT","YEARWEEK"], ascending=[True,True,True],inplace=True)
seloutData.head()


# ## 인덱스 정렬

# In[13]:


seloutData = seloutData.reset_index(drop=True)


# In[14]:


seloutData.head()


# ## 이동평균(판매추세량) 함수
# 이동평균 구간(window=17)로 한 이유는 이상치를 제거하기 위함. 이상치란 상한선보다 크거나 하한선보다 작은 값을 가지는 분석 대상 건이며 이상치는 상한선 또는 하한선으로 보정을 해준다.

# In[15]:


def rollMafunction(data):
    
    # 인덱스 초기화
    data.reset_index(drop=True, inplace=True)
    # rolling 수행
    data["MA"] = data["QTY_NEW"].rolling(window=17, center=True, min_periods=1).mean()
    return data


# In[16]:


groupData = seloutData.groupby(["REGIONID","PRODUCT"]).apply(rollMafunction)
groupData.head(10)


# ## 변동률 구하기 (표준편차)

# In[17]:


def stdMafunction(data):
    
    # 인덱스 초기화
    data.reset_index(drop=True, inplace=True)
    # rolling 수행
    data["STD"] = data["MA"].rolling(window=5, center=True, min_periods=1).std()
    return data


# In[18]:


groupData = groupData.groupby(["REGIONID","PRODUCT"]).apply(stdMafunction)
groupData.head(10)


# ## 상/하한선 구하기

# In[19]:


groupData["UPPER_BOUND"] = groupData["MA"]+groupData["STD"]
groupData["LOWER_BOUND"] = groupData["MA"]-groupData["STD"]
groupData.head()


# ## 정제된 판매량 구하기

# In[20]:


groupData["REFIND_QTY"] = np.where(groupData["QTY_NEW"]>groupData["UPPER_BOUND"], groupData["UPPER_BOUND"],                                    np.where(groupData["QTY_NEW"]<groupData["LOWER_BOUND"], groupData["LOWER_BOUND"],groupData["QTY_NEW"])
                                  )
groupData.head()


# ## 스무딩(추세선)처리 구하기

# In[21]:


def smoothMafunction(data):
    
    # 인덱스 초기화
    data.reset_index(drop=True, inplace=True)
    # rolling 수행
    data["SMOOTH"] = data["REFIND_QTY"].rolling(window=5, center=True, min_periods=1).mean()
    return data


# In[22]:


groupData = groupData.groupby(["REGIONID","PRODUCT"]).apply(smoothMafunction)
groupData.head(10)


# ## 계절성지수산출(안정된시장/불안정시작)

# ### 안정된 시장
# 안정된 시장 = 실제판매량 / 스무딩처리

# In[23]:


groupData["SEASON_JISU1"] = groupData["QTY_NEW"] / groupData["SMOOTH"]
groupData.head()


# ### 불안정 시장
# 불안정시장 = 정제된판매량/스무딩처리

# In[29]:


groupData["SEASON_JISU2"] = groupData["REFIND_QTY"] / groupData["SMOOTH"]
groupData.head()


# ## Postgresql Connection

# In[25]:


# DB 커넥션 열기
postgreEngine = create_engine('postgresql://postgres:test1@127.0.0.1:5432/postgres') 
#postgreEngine = create_engine('postgresql://postgres:postgres@10.184.9.159:5432/postgres') 


# In[26]:


# DB 테이블을 읽어 Data Frame 변수에 저장하기
resultname='seasonality_koingobk'
groupData.to_sql(resultname, postgreEngine, if_exists='replace', index=False)


# In[30]:


# 입력된 데이터 확인하기
postgreResultData = pd.read_sql_query('SELECT * FROM seasonality_koingobk', postgreEngine) 


# In[31]:


postgreResultData.head()
#len(postgreResultData)

