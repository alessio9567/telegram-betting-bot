import requests
import datetime
import CommonUtility,Utility
import Results
import pandas as pd
import mysql.connector
import os
import sys

DB_USER = CommonUtility.getParameterFromFile( 'DATABASE_USER' )
DB_PORT = CommonUtility.getParameterFromFile( 'DATABASE_PORT' )
DB_PWD = CommonUtility.getParameterFromFile( 'DATABASE_PWD' )
DB_SCHEMA = CommonUtility.getParameterFromFile( 'DATABASE_SCHEMA' )
ENV = CommonUtility.getParameterFromFile('ENV')
LEAGUES_LIST_PATH = os.path.dirname(os.getcwd())
HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}


mydb = mysql.connector.connect( user=DB_USER , password = DB_PWD , host = '127.0.0.1' , auth_plugin = 'mysql_native_password' )
mycursor = mydb.cursor()

#DROP TABLE metodo_results;
#CREATE TABLE `metodo_results` (
#  `home_team` varchar(255) DEFAULT NULL,
#  `away_team` varchar(255) DEFAULT NULL,
#  `home_odd` float DEFAULT NULL,
#  `draw_odd` float DEFAULT NULL,
#  `away_odd` float DEFAULT NULL,
#  `match_date` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
#  `home_goals` int(11) DEFAULT NULL,
#  `away_goals` int(11) DEFAULT NULL,
#  `competition_cod` varchar(10) DEFAULT NULL,
#  `match_cod` varchar(30) DEFAULT NULL,
#  `last_lost_odd` float DEFAULT NULL,
#  `last_lost_home` int(1) DEFAULT NULL,
#  `last_competition_cod` varchar(5) DEFAULT NULL,
#  `last_diff_goals` int(11) DEFAULT NULL,
#  `tip` varchar(2) DEFAULT NULL,
#  `is_same_competition` int(1) DEFAULT NULL,
#  `home_result` int(1) DEFAULT NULL,
#  `draw_result` int(1) DEFAULT NULL,
#  `away_result` int(1) DEFAULT NULL,
#  `over_goals` int(1) DEFAULT NULL   
#); 

mycursor.execute("insert into {}.metodo_results \
                  select b.home_team,\
                         b.away_team,\
                         b.home_odd,\
                         b.draw_odd,\
                         b.away_odd,\
                         a.match_date,\
                         b.home_goals,\
                         b.away_goals,\
                         a.competition_cod,\
                         a.match_cod,\
                         a.last_lost_odd,\
                         a.last_lost_home,\
                         a.last_competition_cod,\
                         a.last_diff_goals,\
                         a.tip,\
                         a.competition_cod=a.last_competition_cod,\
                         b.home_goals>b.away_goals,\
                         b.home_goals=b.away_goals,\
                         b.home_goals<b.away_goals,\
                         (b.home_goals+b.away_goals)>=3 \
                  from {}.next_metodo a join {}.latest_results b \
                  on a.match_cod=b.match_cod ".format(DB_SCHEMA,DB_SCHEMA,DB_SCHEMA))

mydb.commit()


#mycursor.execute("with view as( select sum(case when home_goals=away_goals then draw_odd-1 else -1 end) as profitto,\
#                                       count(*) as num_tot_partite,\
#                                       sum(case when home_goals=away_goals then 1 else 0 end)  as num_pareggi, \
#                                       sum(draw_odd) as draw_sum_odds \
#                                from {}.metodo_results \
#                                where flag=1) \
#                  select round((profitto/num_tot_partite)*100,0) as yield,\
#                         profitto, \
#                         num_tot_partite,\
#                         num_pareggi, \
#                         draw_sum_odds/num_tot_partite as avg_odd \
#                  from view;".format(DB_SCHEMA))
#
#df_report=pd.DataFrame(mycursor.fetchall(),columns=["yield","profitto","num_tot_partite","num_pareggi","avg_odd"])
#
#Utility.telegram_bot_sendtext("\U0001F4CA Xbot Report Balance \n \
#\n \
#Stake on single match is 1 \n \
#\n \
#\U0001F4B0 Profit/Loss: {} \n \
#\n \
#\U0001F4C8 Yield: {}% \n \
#\n \
#\U0001F4A5 Total Picks: {} \n \
#\n \
#\U0001F522 Average Odd: {} \n \
#".format(round(df_report.values[0][1],2),df_report.values[0][0],df_report.values[0][2],round(df_report.values[0][4],2)))
#
#mycursor.execute("with view as( select sum(case when home_goals=away_goals then draw_odd-1 else -1 end) as profitto,\
#                                       count(*) as num_tot_partite,\
#                                       sum(case when home_goals=away_goals then 1 else 0 end)  as num_pareggi, \
#                                       sum(draw_odd) as draw_sum_odds \
#                                from {}.metodo_results \
#                                where flag=1) \
#                  select round((profitto/num_tot_partite)*100,0) as yield,\
#                         profitto, \
#                         num_tot_partite,\
#                         num_pareggi, \
#                         draw_sum_odds/num_tot_partite as avg_odd \
#                  from view;".format(DB_SCHEMA))
#
#df_report=pd.DataFrame(mycursor.fetchall(),columns=["yield","profitto","num_tot_partite","num_pareggi","avg_odd"])
#
#mycursor.execute("with data as( select match_date as day,\
#                                       match_cod ,\
#                                       case when home_goals=away_goals then draw_odd-1 else -1 end as single_profit \
#                                from db_metodo_prod.metodo_results \
#                                group by match_cod ) \
#                       select day, \
#                              sum(single_profit) over (order by day) as cumulative_profit \
#                       from data;")
#
#df_report=pd.DataFrame(mycursor.fetchall(),columns=["Data","Cumulative_Profit"])
#
#import numpy as np
#import seaborn as sns
#import matplotlib.pyplot as plt
#sns.set_style("whitegrid")
#
## Color palette
#blue, = sns.color_palette("muted", 1)
#
## Create data
#x = list(range(len(df_report)))
#y = df_report['Cumulative_Profit']
#
## Make the plot
#fig, ax = plt.subplots()
#ax.plot(x, y, color=blue, lw=3)
#ax.fill_between(x, 0, y, alpha=.3)
#ax.set(xlim=(0, len(x)+1), ylim=(-5, None), xticks=x)
#ax.set_xlabel('#Pick')
#ax.set_ylabel('Cumulative_Profit')
#fig.savefig('report_image.jpg')
#
#Utility.telegram_bot_sendphoto('report_image.jpg')
#
