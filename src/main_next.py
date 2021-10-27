
import requests
import datetime
import pandas as pd
import Next,Metodo,CommonUtility,Utility
import os
import sys
import mysql.connector

DB_USER = CommonUtility.getParameterFromFile('DATABASE_USER')
DB_PORT = CommonUtility.getParameterFromFile('DATABASE_PORT')
DB_PWD = CommonUtility.getParameterFromFile('DATABASE_PWD')
DB_SCHEMA = CommonUtility.getParameterFromFile('DATABASE_SCHEMA')
ENV = CommonUtility.getParameterFromFile('ENV')
LEAGUES_LIST_PATH = os.path.dirname(os.getcwd())

mydb = mysql.connector.connect(user=DB_USER, password=DB_PWD,host='127.0.0.1',auth_plugin='mysql_native_password')
mycursor = mydb.cursor()
now = datetime.datetime.now()

df_leagues = pd.read_csv(r'{}/Leagues_next.csv'.format(LEAGUES_LIST_PATH),sep=';',header=None)
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}

Utility.telegram_bot_sendlog("START main_next.py \
Ambiente di {}".format(ENV))

if len(sys.argv) == 1:

    for i in df_leagues.index:

        cl = df_leagues.values[i][0]+'/'+df_leagues.values[i][1]
        print(cl)

        mycursor.execute("delete \
                         from {}.next \
                         where competition_cod='{}'".format(DB_SCHEMA,df_leagues.values[i][2]))
        mydb.commit()

        Next.execute(df_leagues.values[i][0],
                     df_leagues.values[i][1],
                     df_leagues.values[i][2],
                     df_leagues.values[i][3])

        Metodo.execute(df_leagues.values[i][2])

        Utility.telegram_bot_sendlog("END Update Next {}".format(cl))

else:
    cl = sys.argv[1] + '/' + sys.argv[2]
    print(cl)

    mycursor.execute("delete \
                      from {}.next \
                      where competition_cod='{}'".format(DB_SCHEMA, sys.argv[3] ))

    mydb.commit()

    Next.execute( sys.argv[1],
                  sys.argv[2],
                  sys.argv[3],
                  int(sys.argv[4]))

    Metodo.execute(sys.argv[3])



