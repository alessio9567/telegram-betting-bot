import smtplib
import CommonUtility
import mysql.connector
import os
import sys
import csv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase


DB_USER = CommonUtility.getParameterFromFile( 'DATABASE_USER' )
DB_PORT = CommonUtility.getParameterFromFile( 'DATABASE_PORT' )
DB_PWD = CommonUtility.getParameterFromFile( 'DATABASE_PWD' )
DB_SCHEMA = CommonUtility.getParameterFromFile( 'DATABASE_SCHEMA' )
LEAGUES_LIST_PATH = os.path.dirname(os.getcwd())

#Email Variables
SMTP_SERVER = 'smtp.gmail.com' #Email Server (don't change!)
SMTP_PORT = 587 #Server Port (don't change!)
GMAIL_USERNAME = 'raspbotbet@gmail.com' #change this to match your gmail account
GMAIL_PASSWORD = 'monzascommesse888'  #change this to match your gmail password
 
class Emailer:
    def sendmail(self, recipient, subject):
          
        emailData = MIMEMultipart()
        emailData['Subject'] = subject
        emailData['To'] = ", ".join(recipient)
        emailData['From'] = GMAIL_USERNAME
        
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open("/home/pi/metodo/metodo_results.csv", "rb").read())  
        part.add_header('Content-Disposition', 'attachment; filename="metodo_results.csv"')

        emailData.attach(part)
      
        #Connect to Gmail Server
        session = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        session.ehlo()
        session.starttls()
        session.ehlo()
  
        #Login to Gmail
        session.login(GMAIL_USERNAME, GMAIL_PASSWORD)
  
        #Send Email & Exit
        session.sendmail(GMAIL_USERNAME, recipient, emailData.as_string())
        session.quit
  
sender = Emailer()

mydb = mysql.connector.connect( user=DB_USER , password = DB_PWD , host = '127.0.0.1' , auth_plugin = 'mysql_native_password' )
mycursor = mydb.cursor()

mycursor.execute("insert into {}.metodo_results \
                  select b.home_team, \
                         b.away_team, \
                         a.home_odd, \
                         a.draw_odd, \
                         a.away_odd, \
                         b.match_date, \
                         b.tip, \
                         b.competition_cod, \
                         a.home_goals, \
                         a.away_goals, \
                         b.last_lost_home, \
                         b.last_diff_goals, \
                         b.last_lost_odd, \
                         b.last_competition_cod ,\
                         b.match_cod \
                  from {}.latest_results a \
                  join {}.next_metodo b \
                  on a.match_cod = b.match_cod".format(DB_SCHEMA, DB_SCHEMA, DB_SCHEMA))

mydb.commit()

mycursor.execute("select distinct * \
                  from {}.metodo_results \
                  group by match_cod,tip".format(DB_SCHEMA))

results49 = mycursor.fetchall()
fp = open('/home/pi/metodo/metodo_results.csv', 'w')
attach_file = csv.writer(fp)
attach_file.writerows(results49)
fp.close()
#
#
sendTo = [ 'sacchinit@gmail.com', 'alessio.iannini@gmail.com']
emailSubject = "Risultati del bot progressivi"
 
sender.sendmail(sendTo, emailSubject)  
##
##
