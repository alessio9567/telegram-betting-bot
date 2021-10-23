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

def execute(page_content,competition_cod):

    squadre_casa = page_content.findAll('div',attrs={"class":"hometeam"})
    squadre_trasferta = page_content.findAll('div',attrs={"class":"awayteam"})
    prediction_1x2 = page_content.findAll('td',attrs={"class":"center yellow"})
    prediction_goals_and_res = page_content.findAll('td',attrs={"class":"center32"})[6:]


    df_soccvista_pred=pd.DataFrame(index=range(len(squadre_casa)),columns=['Home_Team','Away_Team',
                                                                           '1x2_Tip','OvUn_Tip','ExRes_Tip'])
    i=0
    j=0

    while(i<len(squadre_casa)):

        df_soccvista_pred.values[i][0]=squadre_casa[j].text
        df_soccvista_pred.values[i][1]=squadre_trasferta[j].text
        df_soccvista_pred.values[i][2]=prediction_1x2[j].text
        df_soccvista_pred.values[i][3]=CommonUtility.getParameterFromFile(prediction_goals_and_res[6*j+4].text)
        df_soccvista_pred.values[i][4]=prediction_goals_and_res[6*j+5].text

        i=i+1
        j=j+1



    mycursor.execute("DELETE FROM {}.soccervista_predictions \
                      WHERE competition_cod ='{}'".format(DB_SCHEMA,
                                                          competition_cod))

    sql = "INSERT INTO {}.soccervista_predictions (home_team,\
                                                   away_team,\
                                                   1x2_tip,\
                                                   OvUn_tip,\
                                                   ExRes_tip,\
                                                   competition_cod) VALUES (%s,%s,%s,%s,%s,%s)".format(DB_SCHEMA)
    for i in df_soccvista_pred.index:
        val = (df_soccvista_pred.values[i][0],df_soccvista_pred.values[i][1],df_soccvista_pred.values[i][2],\
               df_soccvista_pred.values[i][3],df_soccvista_pred.values[i][4],competition_cod)
        mycursor.execute(sql, val)
        mydb.commit()


