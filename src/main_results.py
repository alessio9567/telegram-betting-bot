
import requests
import hashlib
from bs4 import BeautifulSoup
import datetime
import CommonUtility,Utility
import Results
import pandas as pd
import mysql.connector
import os

DB_USER = CommonUtility.getParameterFromFile( 'DATABASE_USER' )
DB_PORT = CommonUtility.getParameterFromFile( 'DATABASE_PORT' )
DB_PWD = CommonUtility.getParameterFromFile( 'DATABASE_PWD' )
DB_SCHEMA = CommonUtility.getParameterFromFile( 'DATABASE_SCHEMA' )
ENV = CommonUtility.getParameterFromFile('ENV')
LEAGUES_LIST_PATH = os.path.dirname(os.getcwd())
HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}

mydb = mysql.connector.connect( user=DB_USER , password = DB_PWD , host = '127.0.0.1' , auth_plugin = 'mysql_native_password' )
mycursor = mydb.cursor()

now = datetime.datetime.now()
df_leagues = pd.read_csv(r'{}/Leagues.csv'.format(LEAGUES_LIST_PATH),sep=';',header=None)

for i in df_leagues.index:

    Results.execute(df_leagues.values[i][0],
                    df_leagues.values[i][1],
                    df_leagues.values[i][2],
                    df_leagues.values[i][3])

    print(df_leagues.values[i][0]+'/'+df_leagues.values[i][1])

    Utility.telegram_bot_sendlog("END Update Results {}".format(df_leagues.values[i][0]+'/'+df_leagues.values[i][1]))
