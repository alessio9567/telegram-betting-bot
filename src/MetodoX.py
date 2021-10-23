
import pandas as pd
import mysql.connector
import Utility,CommonUtility
import sys
from sqlalchemy import create_engine

DB_USER=CommonUtility.getParameterFromFile('DATABASE_USER')
DB_PORT=CommonUtility.getParameterFromFile('DATABASE_PORT')
DB_PWD=CommonUtility.getParameterFromFile('DATABASE_PWD')
DB_SCHEMA=CommonUtility.getParameterFromFile('DATABASE_SCHEMA')


def execute(competition_cod):

    mydb = mysql.connector.connect(user=DB_USER, password=DB_PWD,host='127.0.0.1',auth_plugin='mysql_native_password')
    mycursor = mydb.cursor()

    try:
        mycursor.execute("select a.* \
                          from {}.next a \
                          left join {}.next_metodo b \
                          on a.match_cod = b.match_cod \
                          and a.competition_cod = b.competition_cod \
                          where b.match_cod is null \
                          and a.competition_cod='{}'".format(DB_SCHEMA,DB_SCHEMA,competition_cod))

        df_next=pd.DataFrame(mycursor.fetchall(),columns=["home_team","away_team",
                                                          "home_odd","draw_odd","away_odd",
                                                          "match_date","competition_cod","match_cod"])
    except:
        pass
    
    try:
        for i in df_next.index:

            #check for home_team
            home = 0
            mycursor.execute("select * from {}.latest_results \
                              where home_team='{}' or away_team='{}' \
                              order by match_date desc limit 1".format(DB_SCHEMA,df_next.values[i][0],df_next.values[i][0]))

            last_match_home_team=pd.DataFrame(mycursor.fetchall(),columns=["home_team","away_team",
                                                                           "home_odd","draw_odd","away_odd",
                                                                           "home_team_goals","away_team_goals",
                                                                           "match_date","competition_cod","match_cod"])

            diff_goals_home_team=last_match_home_team.values[0][6]-last_match_home_team.values[0][5]

            if(df_next.values[i][0] == last_match_home_team.values[0][0]):

                if(diff_goals_home_team>=3):

                    home = 1

            elif(df_next.values[i][0] == last_match_home_team.values[0][1]):

                if(diff_goals_home_team <=-3):

                    home = 1

            #check for away_team
            away = 0
            mycursor.execute("select * from {}.latest_results \
                              where home_team='{}' or away_team='{}' \
                              order by match_date desc limit 1".format(DB_SCHEMA, df_next.values[i][1], df_next.values[i][1]))

            last_match_away_team=pd.DataFrame(mycursor.fetchall(),columns=["home_team","away_team",
                                                                           "home_odd","draw_odd","away_odd",
                                                                           "home_team_goals","away_team_goals",
                                                                           "match_date","competition_cod","match_cod"])

            diff_goals_away_team = last_match_away_team.values[0][6]-last_match_away_team.values[0][5]
            
            if(df_next.values[i][1] == last_match_away_team.values[0][0]):

                if(diff_goals_away_team>=3):
                    
                    away = 1

            elif(df_next.values[i][1]==last_match_away_team.values[0][1]):

                if(diff_goals_away_team<=-3):

                    away = 1

            if( home == 1 &  away == 1 ):
                bot_message = Utility.build_bot_message(df_next.values[i], last_match_away_team.values[0], 'X')

    except:
        pass

if __name__ == '__main__':
    execute(sys.argv[1])
