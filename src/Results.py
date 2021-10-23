
import requests
from bs4 import BeautifulSoup
import hashlib
import datetime
import CommonUtility
import pandas as pd
import mysql.connector
import sys

DB_USER=CommonUtility.getParameterFromFile('DATABASE_USER')
DB_PORT=CommonUtility.getParameterFromFile('DATABASE_PORT')
DB_PWD=CommonUtility.getParameterFromFile('DATABASE_PWD')
DB_SCHEMA=CommonUtility.getParameterFromFile('DATABASE_SCHEMA')
LEAGUES_LIST_PATH=CommonUtility.getParameterFromFile('LEAGUES_LIST_PATH')

mydb = mysql.connector.connect(user=DB_USER, password=DB_PWD,host='127.0.0.1',auth_plugin='mysql_native_password')
mycursor = mydb.cursor()

now=datetime.datetime.now()

def execute2(N,competition_cod,page_content,risultati):

    giorno_ora=page_content.findAll('td',attrs={"class":"h-text-right h-text-no-wrap"})
    partite=page_content.findAll('td',attrs={"class":"h-text-left"})
    partite_codice = page_content.findAll('a',attrs={"class":"in-match"})

    df=pd.DataFrame(index=range(N),columns=['HomeTeam','AwayTeam',
                                            '1','X','2','HomeTeamGoals','AwayTeamGoals',
                                            'Data','ToDrop','competition_cod','match_cod'])

    for i in df.index:
        ris=risultati[i].text
        if( ('POSTP.' in ris) or ('ABN.' in ris) or ('CAN.' in ris) or ('AWA.' in ris) or ('WO.' in ris) ):
            df.values[i][8]='Y'
        else:
            if( ('PEN' in ris) or ('ET' in ris) ) :
                df.values[i][5]=0
                df.values[i][6]=0
            else:
                ris=ris.split(':')
                df.values[i][5]=int(ris[0])
                df.values[i][6]=int(ris[1])

        time = giorno_ora[i].text
        if(time == 'Today'):
            df.values[i][7]=now.strftime("%Y-%m-%d")
        elif(time == 'Yesterday'):
            df.values[i][7]=(datetime.date.today()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
        else:
            if(len(time) == 6):
                df.values[i][7]=now.strftime("%Y") + '-' + time[3:5] + '-' + time[0:2]
            else:
                df.values[i][7] = time[6:10] + '-' + time[3:5] + '-' + time[0:2]

        matches = partite[i].text.split(' - ')
        df.values[i][0] = matches[0]
        df.values[i][1] = matches[1]

        df.values[i][0]=str(df.values[i][0])
        df.values[i][0]=df.values[i][0].rstrip()
        df.values[i][1]=str(df.values[i][1])
        df.values[i][1]=df.values[i][1].lstrip()
        df.values[i][9]=competition_cod
        df.values[i][10]=partite_codice[i]["href"][len(partite_codice[i]["href"])-9:len(partite_codice[i]["href"])-1]

    i=0
    j=0
    try:
        while(i<=N-1):
            k=2
            while(k<5):
                try:
                    df.values[i][k]=page_content.select("td")[j+k]["data-odd"]
                except:
                    try:
                        df.values[i][k]=page_content.select("td")[j+k].span.span.span["data-odd"]
                    except AttributeError:
                        df.values[i][8]='Y'
                k=k+1
            j=j+6
            i=i+1
    except:
        pass

    sql = "INSERT INTO {}.latest_results (Home_Team,Away_Team,\
                                              Home_Odd,Draw_Odd,Away_Odd,\
                                              Home_Goals,Away_Goals,\
                                              Match_Date,Competition_cod,Match_cod) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(DB_SCHEMA)
    for i in df.index:
        if(df.values[i][8]!='Y'):
            val = (df.values[i][0],df.values[i][1],df.values[i][2],\
                   df.values[i][3],df.values[i][4],df.values[i][5],\
                   df.values[i][6],df.values[i][7],df.values[i][9],df.values[i][10])
            mycursor.execute(sql, val)
            mydb.commit()



def execute(country,league,competition_cod,num_squadre):

    nl = country+'/'+league
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
    page = requests.get("https://www.betexplorer.com/soccer/{}/results".format(nl),headers=headers)
    page_content = BeautifulSoup(page.content, "html.parser")

    if( len( page_content.findAll('li',attrs={"class":"list-tabs__item"} )) == 5 ):
        stage_name='Main'
        page_hash = str(page_content.findAll('td',attrs={"class":"h-text-left"})).encode("utf-8")
        md5_hash = hashlib.md5()
        md5_hash.update(page_hash)
        actual_hash = md5_hash.hexdigest()

        mycursor.execute("select hash \
                          from {}.hash_table \
                          where competition_cod = '{}'".format(DB_SCHEMA,competition_cod+'_'+stage_name))
        try:
            if( actual_hash != mycursor.fetchall()[0][0] ):
                risultati=page_content.findAll('td',attrs={"class":"h-text-center"})
                N = len(risultati)
             
                if N > 0:
                    if N > int(num_squadre):
                        execute2(int(num_squadre),competition_cod,page_content,risultati)
                    else:
                        execute2(N,competition_cod,page_content,risultati)
                
                mycursor.execute("REPLACE INTO {}.hash_table \
                                  SET competition_cod = '{}', hash = '{}'".format(DB_SCHEMA, competition_cod+'_'+stage_name , actual_hash))
                mydb.commit()
            
        except:
            risultati=page_content.findAll('td',attrs={"class":"h-text-center"})
            N = len(risultati)
         
            if N > 0:
                if N > int(num_squadre):
                    execute2(int(num_squadre),competition_cod,page_content,risultati)
                else:
                    execute2(N,competition_cod,page_content,risultati)
            
            mycursor.execute("REPLACE INTO {}.hash_table \
                              SET competition_cod = '{}', hash = '{}'".format(DB_SCHEMA, competition_cod+'_'+stage_name , actual_hash))
            mydb.commit()
                    
    else:
        t=5
        while( t < len(page_content.findAll('li',attrs={"class":"list-tabs__item"})) ):
            stage_cod=page_content.findAll('li',attrs={"class":"list-tabs__item"})[t].a["href"]
            stage_name=page_content.findAll('li',attrs={"class":"list-tabs__item"})[t].text.replace(' ','_')
            
            page=requests.get("https://www.betexplorer.com/soccer/{}/results/{}".format(nl,stage_cod),headers=headers)
            page_content = BeautifulSoup(page.content, "html.parser")
            
            page_hash = str(page_content.findAll('td',attrs={"class":"h-text-left"})).encode("utf-8")
            md5_hash = hashlib.md5()
            md5_hash.update(page_hash)
            actual_hash = md5_hash.hexdigest()

            mycursor.execute("select hash \
                              from {}.hash_table \
                              where competition_cod = '{}'".format(DB_SCHEMA,competition_cod+'_'+stage_name))
          
            
            try:
                if( actual_hash != mycursor.fetchall()[0][0] ):

                    risultati=page_content.findAll('td',attrs={"class":"h-text-center"})

                    N=len(risultati)
                    
                    if N > int(num_squadre):
                        execute2(int(num_squadre),competition_cod,page_content,risultati)
                    else:
                        execute2(N,competition_cod,page_content,risultati)
                    
                    mycursor.execute("REPLACE INTO {}.hash_table \
                                      SET competition_cod = '{}', hash = '{}'".format(DB_SCHEMA, competition_cod+'_'+stage_name , actual_hash))
                    mydb.commit()
            except:

                risultati=page_content.findAll('td',attrs={"class":"h-text-center"})

                N=len(risultati)

                if N > int(num_squadre):
                    execute2(int(num_squadre),competition_cod,page_content,risultati)
                else:
                    execute2(N,competition_cod,page_content,risultati)

                mycursor.execute("REPLACE INTO {}.hash_table \
                                  SET competition_cod = '{}', hash = '{}'".format(DB_SCHEMA, competition_cod+'_'+stage_name , actual_hash))
                mydb.commit()

            t=t+1


if __name__ == '__main__':
    execute(sys.argv[1],sys.argv[2],sys.argv[3],int(sys.argv[4]))

