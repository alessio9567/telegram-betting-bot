
import pandas as pd
import mysql.connector
import Utility,CommonUtility
import sys
from difflib import SequenceMatcher

DB_USER=CommonUtility.getParameterFromFile('DATABASE_USER')
DB_PORT=CommonUtility.getParameterFromFile('DATABASE_PORT')
DB_PWD=CommonUtility.getParameterFromFile('DATABASE_PWD')
DB_SCHEMA=CommonUtility.getParameterFromFile('DATABASE_SCHEMA')

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def insertIntoNextMetodo( df_next_metodo, last_match_home_team, last_lost_home, last_competition_cod,last_diff_goals, tip):

    mydb = mysql.connector.connect(user=DB_USER,
                                   password=DB_PWD,
                                   host='127.0.0.1',
                                   auth_plugin='mysql_native_password')
    mycursor = mydb.cursor()

    sql = "INSERT INTO {}.next_metodo (Home_Team,Away_Team,\
                                       Home_Odd,Draw_Odd,Away_Odd,\
                                       Match_Date,Competition_Cod,Match_Cod,\
                                       last_lost_odd,last_lost_home,last_competition_cod,last_diff_goals,tip) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(DB_SCHEMA)

    val = (df_next_metodo[0],df_next_metodo[1],\
           df_next_metodo[2],df_next_metodo[3],df_next_metodo[4],\
           str(df_next_metodo[5]),df_next_metodo[6],df_next_metodo[7],\
           last_match_home_team,last_lost_home,last_competition_cod,last_diff_goals,tip)

    mycursor.execute(sql, val)
    mydb.commit()

def execute(competition_cod):

    mydb = mysql.connector.connect(user=DB_USER,
                                   password=DB_PWD,
                                   host='127.0.0.1',
                                   auth_plugin='mysql_native_password')
    mycursor = mydb.cursor()

    try:
        mycursor.execute("select * \
                          from {}.next \
                          where competition_cod='{}' \
                          group by match_cod \
                          order by match_date asc".format(DB_SCHEMA,competition_cod))

        df_next=pd.DataFrame(mycursor.fetchall(),columns=["home_team","away_team",
                                                          "home_odd","draw_odd","away_odd",
                                                          "match_date","competition_cod","match_cod"])
        mycursor.execute("select * \
                          from {}.soccervista_predictions \
                          where competition_cod='{}'".format(DB_SCHEMA,competition_cod))

        df_soccervista_predictions=pd.DataFrame(mycursor.fetchall(),columns=["home_team","away_team",
                                                                             "1x2_tip","OvUn_tip","ExRes_tip",
                                                                             "competition_cod"])
        mycursor.execute("select * \
                          from {}.rankings \
                          where competition_cod='{}'".format(DB_SCHEMA,competition_cod))

        df_rankings=pd.DataFrame(mycursor.fetchall(),columns=["team","competition_cod","rank","points","hash"])


        for i in df_next.index:

            #check for home_team

            mycursor.execute("select * from {}.latest_results \
                              where home_team='{}' or away_team='{}' \
                              order by match_date desc limit 1".format(DB_SCHEMA,
                                                                       df_next.values[i][0],
                                                                       df_next.values[i][0]))

            last_match_home_team=pd.DataFrame(mycursor.fetchall(),columns=["home_team","away_team",
                                                                           "home_odd","draw_odd","away_odd",
                                                                           "home_team_goals","away_team_goals",
                                                                           "match_date","competition_cod","match_cod"])

            diff_goals_home_team=last_match_home_team.values[0][6]-last_match_home_team.values[0][5]

            if(diff_goals_home_team>=3):

                if((df_next.values[i][0] == last_match_home_team.values[0][0]) &
                  (float(df_next.values[i][2]) <= 6) & (float(last_match_home_team.values[0][2]) <= 6)):

                    if(len(df_rankings)>0):
                        rank_arr_home=df_rankings[df_rankings["team"]==df_next.values[i][0]]
                        rank_arr_away=df_rankings[df_rankings["team"]==df_next.values[i][1]]

                        rank_home=rank_arr_home.values[0][2]
                        points_home=rank_arr_home.values[0][3]
                        rank_away=rank_arr_away.values[0][2]
                        points_away=rank_arr_away.values[0][3]
                        number_of_teams=len(df_rankings)
                    else:
                        rank_home=null
                        points_home=null
                        rank_away=null
                        points_away=null
                        number_of_teams=null

                    if(df_soccervista_predictions.empty or len(df_soccervista_predictions)==0):

                        soccervista_prediction = ''
                        soccervista_goals  = ''
                        soccervista_exactresult = ''
                        Utility.build_bot_message(last_match_home_team.values[0],df_next.values[i],soccervista_prediction,soccervista_goals,soccervista_exactresult,tip,rank_home,points_home,rank_away,points_away,number_of_teams)
                        continue
                        #insertIntoNextMetodo( df_next.values[i], last_match_home_team.values[0][2], 1, last_match_home_team.values[0][8], diff_goals_home_team,tip)

                    else:
                        df_next_method=df_next.loc[[i]].merge(df_soccervista_predictions,how='inner',on='competition_cod')

                        df_next_method['Score_Home'] = None
                        df_next_method['Score_Away'] = None
                        df_next_method['Sum_Score'] = None

                        for j in df_next_method.index:
                            df_next_method.at[j,'Score_Home']=similar(df_next_method.values[j][0],df_next_method.values[j][8])
                            df_next_method.at[j,'Score_Away']=similar(df_next_method.values[j][1],df_next_method.values[j][9])
                            df_next_method.at[j,'Sum_Score']=df_next_method.at[j,'Score_Home']+df_next_method.at[j,'Score_Away']

                        df_next_method['Sum_Score']=pd.to_numeric(df_next_method['Sum_Score'])
                        df_next_method=df_next_method.loc[[df_next_method['Sum_Score'].idxmax()]]
                        df_next_method['last_lost_odd'] = last_match_home_team.values[0][2]
                        df_next_method['last_lost_home'] = 1
                        df_next_method['last_competition_cod'] = last_match_home_team.values[0][8]
                        df_next_method['last_diff_goals'] = diff_goals_home_team
                        df_next_method['tip'] = '1X'
                        df_next_method['flag'] = 0
                        df_next_method['home_goals'] = None
                        df_next_method['away_goals'] = None
                        tip = '1X'
                        soccervista_prediction=df_next_method.values[0][10]
                        soccervista_goals=df_next_method.values[0][11]
                        soccervista_exactresult=df_next_method.values[0][12]

                        Utility.build_bot_message(last_match_home_team.values[0],df_next_method.values[0],soccervista_prediction,soccervista_goals,soccervista_exactresult,tip,rank_home,points_home,rank_away,points_away,number_of_teams)
                        continue
                        #insertIntoNextMetodo( df_next_method[0], last_match_home_team.values[0][2], 1, last_match_home_team.values[0][8], diff_goals_home_team,tip)

            elif(diff_goals_home_team <= -3):

                if((df_next.values[i][0] == last_match_home_team.values[0][1]) &
                   (float(df_next.values[i][2]) <= 6) & (float(last_match_home_team.values[0][4]) <= 6)):

                    if(len(df_rankings)>0):
                        rank_arr_home=df_rankings[df_rankings["team"]==df_next.values[i][0]]
                        rank_arr_away=df_rankings[df_rankings["team"]==df_next.values[i][1]]

                        rank_home=rank_arr_home.values[0][2]
                        points_home=rank_arr_home.values[0][3]
                        rank_away=rank_arr_away.values[0][2]
                        points_away=rank_arr_away.values[0][3]
                        number_of_teams=len(df_rankings)
                    else:
                        rank_home=null
                        points_home=null
                        rank_away=null
                        points_away=null
                        number_of_teams=null

                    if(df_soccervista_predictions.empty or len(df_soccervista_predictions)==0):

                        soccervista_prediction = ''
                        soccervista_goals  = ''
                        soccervista_exactresult = ''
                        Utility.build_bot_message(last_match_home_team.values[0],df_next.values[i],soccervista_prediction,soccervista_goals,soccervista_exactresult,'1X',rank_home,points_home,rank_away,points_away,number_of_teams)
                        continue
                        #insertIntoNextMetodo( df_next.values[i], last_match_home_team.values[0][2], 1, last_match_home_team.values[0][8], diff_goals_home_team,tip)

                    else:
                        df_next_method=df_next.loc[[i]].merge(df_soccervista_predictions,how='inner',on='competition_cod')

                        df_next_method['Score_Home'] = None
                        df_next_method['Score_Away'] = None
                        df_next_method['Sum_Score'] = None

                        for j in df_next_method.index:
                            df_next_method.at[j,'Score_Home']=similar(df_next_method.values[j][0],df_next_method.values[j][8])
                            df_next_method.at[j,'Score_Away']=similar(df_next_method.values[j][1],df_next_method.values[j][9])
                            df_next_method.at[j,'Sum_Score']=df_next_method.at[j,'Score_Home']+df_next_method.at[j,'Score_Away']

                        df_next_method['Sum_Score']=pd.to_numeric(df_next_method['Sum_Score'])
                        df_next_method=df_next_method.loc[[df_next_method['Sum_Score'].idxmax()]]
                        df_next_method['last_lost_odd'] = last_match_home_team.values[0][2]
                        df_next_method['last_lost_home'] = 1
                        df_next_method['last_competition_cod'] = last_match_home_team.values[0][8]
                        df_next_method['last_diff_goals'] = diff_goals_home_team
                        df_next_method['tip'] = '1X'
                        df_next_method['flag'] = 0
                        df_next_method['home_goals'] = None
                        df_next_method['away_goals'] = None
                        tip = '1X'
                        soccervista_prediction=df_next_method.values[0][10]
                        soccervista_goals=df_next_method.values[0][11]
                        soccervista_exactresult=df_next_method.values[0][12]

                        Utility.build_bot_message(last_match_home_team.values[0],df_next_method.values[0],soccervista_prediction,soccervista_goals,soccervista_exactresult,tip,rank_home,points_home,rank_away,points_away,number_of_teams)
                        continue
                        #insertIntoNextMetodo( df_next_method[0], last_match_home_team.values[0][2], 1, last_match_home_team.values[0][8], diff_goals_home_team,tip)

            #check for away_team

            mycursor.execute("select * from {}.latest_results \
                              where home_team='{}' or away_team='{}' \
                              order by match_date desc limit 1".format(DB_SCHEMA,
                                                                       df_next.values[i][1],
                                                                       df_next.values[i][1]))

            last_match_away_team=pd.DataFrame(mycursor.fetchall(),columns=["home_team","away_team",
                                                                           "home_odd","draw_odd","away_odd",
                                                                           "home_team_goals","away_team_goals",
                                                                           "match_date","competition_cod","match_cod"])

            diff_goals_away_team = last_match_away_team.values[0][6]-last_match_away_team.values[0][5]

            if(diff_goals_away_team>=3):

                if(( df_next.values[i][1] == last_match_away_team.values[0][0] ) &
                   ( float(df_next.values[i][4]) <= 6 ) & ( float(last_match_home_team.values[0][2]) <= 6 ) ):

                    if(len(df_rankings)>0):
                        rank_arr_home=df_rankings[df_rankings["team"]==df_next.values[i][0]]
                        rank_arr_away=df_rankings[df_rankings["team"]==df_next.values[i][1]]

                        rank_home=rank_arr_home.values[0][2]
                        points_home=rank_arr_home.values[0][3]
                        rank_away=rank_arr_away.values[0][2]
                        points_away=rank_arr_away.values[0][3]
                        number_of_teams=len(df_rankings)
                    else:
                        rank_home=null
                        points_home=null
                        rank_away=null
                        points_away=null
                        number_of_teams=null

                    if(df_soccervista_predictions.empty or len(df_soccervista_predictions)==0):

                        soccervista_prediction = ''
                        soccervista_goals  = ''
                        soccervista_exactresult = ''
                        Utility.build_bot_message(last_match_away_team.values[0],df_next.values[i],soccervista_prediction,soccervista_goals,soccervista_exactresult,'X2',rank_home,points_home,rank_away,points_away,number_of_teams)
                        #insertIntoNextMetodo( df_next.values[i], last_match_home_team.values[0][2], 1, last_match_home_team.values[0][8], diff_goals_home_team,tip)

                    else:
                        df_next_method=df_next.loc[[i]].merge(df_soccervista_predictions,how='inner',on='competition_cod')

                        df_next_method['Score_Home'] = None
                        df_next_method['Score_Away'] = None
                        df_next_method['Sum_Score'] = None

                        for j in df_next_method.index:
                            df_next_method.at[j,'Score_Home']=similar(df_next_method.values[j][0],df_next_method.values[j][8])
                            df_next_method.at[j,'Score_Away']=similar(df_next_method.values[j][1],df_next_method.values[j][9])
                            df_next_method.at[j,'Sum_Score']=df_next_method.at[j,'Score_Home']+df_next_method.at[j,'Score_Away']

                        df_next_method['Sum_Score']=pd.to_numeric(df_next_method['Sum_Score'])
                        df_next_method=df_next_method.loc[[df_next_method['Sum_Score'].idxmax()]]
                        df_next_method['last_lost_odd'] = last_match_home_team.values[0][2]
                        df_next_method['last_lost_home'] = 1
                        df_next_method['last_competition_cod'] = last_match_home_team.values[0][8]
                        df_next_method['last_diff_goals'] = diff_goals_home_team
                        df_next_method['tip'] = '1X'
                        df_next_method['flag'] = 0
                        df_next_method['home_goals'] = None
                        df_next_method['away_goals'] = None
                        tip = 'X2'
                        soccervista_prediction=df_next_method.values[0][10]
                        soccervista_goals=df_next_method.values[0][11]
                        soccervista_exactresult=df_next_method.values[0][12]

                        Utility.build_bot_message(last_match_away_team.values[0],df_next_method.values[0],soccervista_prediction,soccervista_goals,soccervista_exactresult,tip,rank_home,points_home,rank_away,points_away,number_of_teams)
                        #insertIntoNextMetodo( df_next_method[0], last_match_home_team.values[0][2], 1, last_match_home_team.values[0][8], diff_goals_home_team,tip)

            #lost_home=false

            elif(diff_goals_away_team<=-3):

                if(( df_next.values[i][1] == last_match_away_team.values[0][1] ) &
                     ( float(df_next.values[i][4]) <= 6 ) & ( float(last_match_home_team.values[0][4]) <= 6 )):

                    if(len(df_rankings)>0):
                        rank_arr_home=df_rankings[df_rankings["team"]==df_next.values[i][0]]
                        rank_arr_away=df_rankings[df_rankings["team"]==df_next.values[i][1]]

                        rank_home=rank_arr_home.values[0][2]
                        points_home=rank_arr_home.values[0][3]
                        rank_away=rank_arr_away.values[0][2]
                        points_away=rank_arr_away.values[0][3]
                        number_of_teams=len(df_rankings)
                    else:
                        rank_home=null
                        points_home=null
                        rank_away=null
                        points_away=null
                        number_of_teams=null

                    if(df_soccervista_predictions.empty or len(df_soccervista_predictions)==0):

                        soccervista_prediction = ''
                        soccervista_goals  = ''
                        soccervista_exactresult = ''
                        Utility.build_bot_message(last_match_away_team.values[0],df_next.values[i],soccervista_prediction,soccervista_goals,soccervista_exactresult,'X2',rank_home,points_home,rank_away,points_away,number_of_teams)
                        #insertIntoNextMetodo( df_next.values[i], last_match_home_team.values[0][2], 1, last_match_home_team.values[0][8], diff_goals_home_team,tip)

                    else:
                        df_next_method=df_next.loc[[i]].merge(df_soccervista_predictions,how='inner',on='competition_cod')

                        df_next_method['Score_Home'] = None
                        df_next_method['Score_Away'] = None
                        df_next_method['Sum_Score'] = None

                        for j in df_next_method.index:
                            df_next_method.at[j,'Score_Home']=similar(df_next_method.values[j][0],df_next_method.values[j][8])
                            df_next_method.at[j,'Score_Away']=similar(df_next_method.values[j][1],df_next_method.values[j][9])
                            df_next_method.at[j,'Sum_Score']=df_next_method.at[j,'Score_Home']+df_next_method.at[j,'Score_Away']

                        df_next_method['Sum_Score']=pd.to_numeric(df_next_method['Sum_Score'])
                        df_next_method=df_next_method.loc[[df_next_method['Sum_Score'].idxmax()]]
                        df_next_method['last_lost_odd'] = last_match_home_team.values[0][2]
                        df_next_method['last_lost_home'] = 1
                        df_next_method['last_competition_cod'] = last_match_home_team.values[0][8]
                        df_next_method['last_diff_goals'] = diff_goals_home_team
                        df_next_method['tip'] = '1X'
                        df_next_method['flag'] = 0
                        df_next_method['home_goals'] = None
                        df_next_method['away_goals'] = None
                        tip = 'X2'
                        soccervista_prediction=df_next_method.values[0][10]
                        soccervista_goals=df_next_method.values[0][11]
                        soccervista_exactresult=df_next_method.values[0][12]

                        Utility.build_bot_message(last_match_away_team.values[0],df_next_method.values[0],soccervista_prediction,soccervista_goals,soccervista_exactresult,tip,rank_home,points_home,rank_away,points_away,number_of_teams)
                        #insertIntoNextMetodo( df_next_method[0], last_match_home_team.values[0][2], 1, last_match_home_team.values[0][8], diff_goals_home_team,tip)
    except:
        pass

if __name__=='__main__':
    execute(sys.argv[1])


