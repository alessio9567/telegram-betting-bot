import datetime
import CommonUtility
import pandas as pd
import mysql.connector

DB_USER=CommonUtility.getParameterFromFile('DATABASE_USER')
DB_PORT=CommonUtility.getParameterFromFile('DATABASE_PORT')
DB_PWD=CommonUtility.getParameterFromFile('DATABASE_PWD')
DB_SCHEMA=CommonUtility.getParameterFromFile('DATABASE_SCHEMA')
LEAGUES_LIST_PATH=CommonUtility.getParameterFromFile('LEAGUES_LIST_PATH')

mydb = mysql.connector.connect(user=DB_USER,
                               password=DB_PWD,
                               host='127.0.0.1',
                               auth_plugin='mysql_native_password')
mycursor = mydb.cursor()

now=datetime.datetime.now()

def execute(page_content,competition_cod,hash):

    teams = page_content.findAll('td',attrs={"class":"participant_name col_participant_name col_name"})
    points = page_content.findAll('td',attrs={"class":"points col_points"})
    
    df_rankings=pd.DataFrame(index=range(len(teams)),columns=['Team','Points'])
    i=0
    
    while(i<len(teams)):
    
        df_rankings.values[i][0]=teams[i].text
        df_rankings.values[i][1]=points[i].text
    
        i=i+1
    
    
    df_rankings.sort_values(by=['Points'],ascending=False)
    
    mycursor.execute("DELETE FROM {}.rankings \
                      WHERE competition_cod ='{}'".format(DB_SCHEMA,
                                                          competition_cod))
    
    sql = "INSERT INTO {}.rankings (Team,\
                                    Competition_Cod,\
                                    Rank,\
                                    Points,\
                                    Hash) VALUES (%s,%s,%s,%s,%s)".format(DB_SCHEMA)
               
    for i in df_rankings.index:
        val = (df_rankings.values[i][0],competition_cod,i+1,\
               df_rankings.values[i][1],hash)
        mycursor.execute(sql, val)
        mydb.commit()
     
