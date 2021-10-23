import requests
import hashlib
from bs4 import BeautifulSoup
import datetime
import CommonUtility,Utility
import pandas as pd
import mysql.connector
import os
import Rankings

DB_USER = CommonUtility.getParameterFromFile( 'DATABASE_USER' )
DB_PORT = CommonUtility.getParameterFromFile( 'DATABASE_PORT' )
DB_PWD = CommonUtility.getParameterFromFile( 'DATABASE_PWD' )
DB_SCHEMA = CommonUtility.getParameterFromFile( 'DATABASE_SCHEMA' )
ENV = CommonUtility.getParameterFromFile('ENV')
LEAGUES_LIST_PATH = os.path.dirname(os.getcwd())
HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) \
            Chrome/61.0.3163.100 Safari/539.36'}

mydb = mysql.connector.connect( user=DB_USER , password = DB_PWD , host = '127.0.0.1' , auth_plugin = 'mysql_native_password' )
mycursor = mydb.cursor()

Utility.telegram_bot_sendlog("START main_rankings.py")

now = datetime.datetime.now()
df_leagues = pd.read_csv(r'{}/Leagues_rankings.csv'.format(LEAGUES_LIST_PATH),sep=';',header=None)

for i in df_leagues.index:

    country = df_leagues.values[i][0]
    league = df_leagues.values[i][1]
    competition_cod = df_leagues.values[i][2]
    number_of_teams = df_leagues.values[i][3]
    ranking_cod = df_leagues.values[i][4]

    print("{} {}".format(country,league))

    page = requests.get("https://www.betexplorer.com/soccer/{}/{}/standings/?table=table&table_sub=&ts={}check=0".format(country,league,ranking_cod),headers=HEADERS)

    page_content = BeautifulSoup(page.content, "html.parser")

    ranks = str(page_content.findAll('td')).encode("utf-8")
    md5_hash = hashlib.md5()
    md5_hash.update(ranks)
    actual_hash = md5_hash.hexdigest()

    try:
        mycursor.execute("SELECT hash \
                          FROM {}.rankings \
                          WHERE competition_cod = '{}'".format(DB_SCHEMA, competition_cod))
    except Exception as e:
        print(e)

    try:
        if( actual_hash != mycursor.fetchall()[0][0] ):

            Rankings.execute(page_content,competition_cod,actual_hash)

            mycursor.execute("REPLACE INTO {}.rankings \
                              SET competition_cod = '{}', hash = '{}'".format(DB_SCHEMA,competition_cod,actual_hash))
            mydb.commit()
    except Exception as e:
        print(e)

    Utility.telegram_bot_sendlog("END Update Rankings {} {}".format(country,league))

