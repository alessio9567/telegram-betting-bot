
import requests
from bs4 import BeautifulSoup
import datetime
import sys
import CommonUtility,Utility
import pandas as pd
import mysql.connector

DB_USER = CommonUtility.getParameterFromFile('DATABASE_USER')
DB_PORT = CommonUtility.getParameterFromFile('DATABASE_PORT')
DB_PWD = CommonUtility.getParameterFromFile('DATABASE_PWD')
DB_SCHEMA = CommonUtility.getParameterFromFile('DATABASE_SCHEMA')
ENV = CommonUtility.getParameterFromFile('ENV')
mydb = mysql.connector.connect(user=DB_USER, password=DB_PWD,host='127.0.0.1',auth_plugin='mysql_native_password')
mycursor = mydb.cursor()

now=datetime.datetime.now()

def execute(country,league,competition_cod,N):

    nl=country+'/'+league
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}
    page=requests.get("https://www.betexplorer.com/soccer/{}/fixtures".format(nl), headers = headers)
    page_content = BeautifulSoup(page.content, "html.parser")

    if(len(page_content.findAll('li',attrs={"class":"list-tabs__item"}))==5):

        partite1=page_content.findAll('td', attrs={"class":"h-text-left"})
        giorno_ora=page_content.findAll('td', attrs={"class":"table-main__datetime"})
        partite_codice = page_content.findAll('a', attrs={"class":"in-match"})

        if len(partite1)>N:
            dfnext=pd.DataFrame(index=range(int(N)),columns=['HomeTeam','AwayTeam','1','X','2','Giorno','Match_Cod','ToSkip'])
        else:
            dfnext=pd.DataFrame(index=range(len(partite1)),columns=['HomeTeam','AwayTeam','1','X','2','Giorno','Match_Cod','ToSkip'])

        #giorno_ora
        try:
            for r in dfnext.index:
                col = giorno_ora[r].text.split(' ')
                if(len(col) == 2):
                    if(col[0] == 'Today'):
                        dfnext.values[r][5] = now.strftime("%Y-%m-%d")+' '+col[1]+':00'
                    elif(col[0] == 'Tomorrow'):
                        dfnext.values[r][5] = (datetime.date.today()+datetime.timedelta(days=1)).strftime("%Y-%m-%d")+' '+col[1]+':00'
                    else:
                        dfnext.values[r][5] = now.strftime("%Y") + '-' + col[0][3:5] + '-' + col[0][0:2] + ' ' + col[1] + ':00'
                else:
                    dfnext.values[r][5] = dfnext.values[r-1][5]
        except:
            pass
    
        #nomi_squadre
        for i in dfnext.index:
            col = partite1[i].text.split('-')
            dfnext.values[i][0] = col[0]
            dfnext.values[i][1] = col[1]
            dfnext.values[i][6] = partite_codice[i]["href"][len(partite_codice[i]["href"])-9:len(partite_codice[i]["href"])-1]
    
    
        for i in dfnext.index:
            dfnext.values[i][0]=dfnext.values[i][0].rstrip()
            dfnext.values[i][1]=dfnext.values[i][1].lstrip()
            if(abs(datetime.datetime.strptime(dfnext.values[i][5],"%Y-%m-%d %H:%M:%S")-now).days>=3):
                dfnext.values[i][7]='Y'
    
        i=0
        j=0
    
        while(i<=len(dfnext)-1):
            k=2
            while(k<5):
                try:
                    dfnext.values[i][k]=page_content.select("td")[j+k+2].a["data-odd"]
                except TypeError:
                    dfnext.values[i][7]='Y'
                k=k+1
            j=j+7
            i=i+1

        sql = "INSERT INTO {}.next (Home_Team,Away_Team,\
                                    Home_Odd,Draw_Odd,Away_Odd,\
                                    Match_Date,Match_Cod,Competition_Cod) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)".format(DB_SCHEMA)

        for i in dfnext.index:
            if(dfnext.values[i][7]!='Y'):
                val = (dfnext.values[i][0],dfnext.values[i][1],\
                       dfnext.values[i][2],dfnext.values[i][3],dfnext.values[i][4],\
                       dfnext.values[i][5],dfnext.values[i][6],competition_cod)
                mycursor.execute(sql, val)
                mydb.commit()

    else:
        t=5
        while(t<len(page_content.findAll('li',attrs={"class":"list-tabs__item"}))):
            
            stage_cod=page_content.findAll('li',attrs={"class":"list-tabs__item"})[t].a["href"]       
            page=requests.get("https://www.betexplorer.com/soccer/{}/fixtures/{}".format(nl,stage_cod),headers=headers)
            page_content = BeautifulSoup(page.content, "html.parser")
            
            partite1=page_content.findAll('td', attrs={"class":"h-text-left"})
            giorno_ora=page_content.findAll('td', attrs={"class":"table-main__datetime"})
            partite_codice = page_content.findAll('a', attrs={"class":"in-match"})
            
            if len(partite1)>N:
                dfnext=pd.DataFrame(index=range(int(N)),columns=['HomeTeam','AwayTeam','1','X','2','Giorno','Match_Cod','ToSkip'])
            else:
                dfnext=pd.DataFrame(index=range(len(partite1)),columns=['HomeTeam','AwayTeam','1','X','2','Giorno','Match_Cod','ToSkip'])
            
            #giorno_ora
            try:
                for r in dfnext.index:
                    col = giorno_ora[r].text.split(' ')
                    if(len(col) == 2):
                        if(col[0] == 'Today'):
                            dfnext.values[r][5] = now.strftime("%Y-%m-%d")+' '+col[1]+':00'
                        elif(col[0] == 'Tomorrow'):
                            dfnext.values[r][5] = (datetime.date.today()+datetime.timedelta(days=1)).strftime("%Y-%m-%d")+' '+col[1]+':00'
                        else:
                            dfnext.values[r][5] = now.strftime("%Y") + '-' + col[0][3:5] + '-' + col[0][0:2] + ' ' + col[1] + ':00'
                    else:
                        dfnext.values[r][5] = dfnext.values[r-1][5]
            except:
                pass

            #nomi_squadre
            for i in dfnext.index:
                col = partite1[i].text.split('-')
                dfnext.values[i][0] = col[0]
                dfnext.values[i][1] = col[1]
                dfnext.values[i][6] = partite_codice[i]["href"][len(partite_codice[i]["href"])-9:len(partite_codice[i]["href"])-1]

            for i in dfnext.index:
                dfnext.values[i][0]=dfnext.values[i][0].rstrip()
                dfnext.values[i][1]=dfnext.values[i][1].lstrip()
                if(abs(datetime.datetime.strptime(dfnext.values[i][5],"%Y-%m-%d %H:%M:%S")-now).days>=4):
                    dfnext.values[i][7]='Y'
            
            i=0
            j=0

            while(i<=len(dfnext)-1):
                k=2
                while(k<5):
                    try:
                        dfnext.values[i][k]=page_content.select("td")[j+k+2].a["data-odd"]
                    except TypeError:
                        dfnext.values[i][7]='Y'
                    k=k+1
                j=j+7
                i=i+1

            t=t+1

            sql = "INSERT INTO {}.next (Home_Team,Away_Team,\
                                        Home_Odd,Draw_Odd,Away_Odd,\
                                        Match_Date,Match_Cod,Competition_Cod) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)".format(DB_SCHEMA)

            for i in dfnext.index:
                if(dfnext.values[i][7]!='Y'):
                    val = (dfnext.values[i][0],dfnext.values[i][1],\
                           dfnext.values[i][2],dfnext.values[i][3],dfnext.values[i][4],\
                           dfnext.values[i][5],dfnext.values[i][6],competition_cod)
                    mycursor.execute(sql, val)
                    mydb.commit()


if __name__ == '__main__':
    execute(sys.argv[1],sys.argv[2],sys.argv[3],int(sys.argv[4]))
