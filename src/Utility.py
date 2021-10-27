import requests
import mysql.connector
import CommonUtility
from crontab import CronTab
from datetime import datetime,timedelta

DB_USER = CommonUtility.getParameterFromFile( 'DATABASE_USER' )
DB_PORT = CommonUtility.getParameterFromFile('DATABASE_PORT')
DB_PWD = CommonUtility.getParameterFromFile('DATABASE_PWD')
DB_SCHEMA = CommonUtility.getParameterFromFile('DATABASE_SCHEMA')

mydb = mysql.connector.connect(user=DB_USER, password=DB_PWD, host='127.0.0.1', auth_plugin='mysql_native_password' )
mycursor = mydb.cursor()

def scheduler( input_file_name ):

    file_name = input_file_name.split('_')

    d_string = file_name[4] + '-' + file_name[3] + '-' + file_name[2] +' ' + file_name[5]

    date_time_obj = datetime.strptime(d_string, '%Y-%m-%d %H:%M')
    date_time_obj = date_time_obj - timedelta(hours=1,minutes=15)

    cron = CronTab(user='pi')
    job_command='sh /home/pi/metodoprod/src/Notify.sh {}.txt'.format(input_file_name)


    if(job_command not in list(cron.commands)):
        job = cron.new(command=job_command)
        job.setall(date_time_obj.minute,
                   date_time_obj.hour,
                   date_time_obj.day,
                   date_time_obj.month,
                   None)

        cron.write()



def build_bot_message( last_match, method_match,soccervista_prediction,soccervista_goals,soccervista_exactresult,tip,rank_home,points_home,rank_away,points_away,number_of_teams ):

    bot_message_last_match = "\n \
\U0001F4C5 {} \n \
{} \n \
\U000026BD {}-{} \n \
\U00002714 {}:{} \n \
\U0001F3B2 @{} @{} @{} \n \
-------------------------------- \n".format(str(last_match[7].day)+'/'+str(last_match[7].month)+'/'+str(last_match[7].year),
                                            CommonUtility.getParameterFromFile(last_match[8]),
                                            last_match[0],
                                            last_match[1],
                                            last_match[5],
                                            last_match[6],
                                            last_match[2],
                                            last_match[3],
                                            last_match[4])


    bot_message_next_match= "\n \
\U0001F4C5 {} \U0000231A {} \n \
\n \
{} \n \
\n \
\U000026BD {}-{} \n \
\n \
\U0001F3B2 @{} @{} @{} \n  \
\n \
\U0001F449 Tip: {} \n \
\n \
Rankings: {}° vs {}° \n \
Points: {} pts vs {} pts \n \
Top {}% vs Top {}% \n \
\n \
Soccervista Prediction: \n \
{} \n \
{} \n \
{}".format(str(method_match[5]).split(' ')[0][8:10]+'/'+str(method_match[5]).split(' ')[0][5:7]+'/'+str(method_match[5]).split(' ')[0][:4],
           str(method_match[5]).split(' ')[1][:5],
           CommonUtility.getParameterFromFile(method_match[6]),
           method_match[0],
           method_match[1],
           method_match[2],
           method_match[3],
           method_match[4],
           tip,
           int(rank_home),
           int(rank_away),
           int(points_home),
           int(points_away),
           round((rank_home/number_of_teams)*100,0),
           round((rank_away/number_of_teams)*100,0),
           soccervista_prediction,
           soccervista_goals,
           soccervista_exactresult)

    telegram_bot_send_message( bot_message_last_match + bot_message_next_match )

    file_name = "{}_{}_{}_{}".format(method_match[0].replace(' ','-'),
                                     method_match[1].replace(' ','-'),
                                     str(method_match[5]).split(' ')[0][8:10]+'_'+str(method_match[5]).split(' ')[0][5:7]+'_'+str(method_match[5]).split(' ')[0][:4],
                                     str(method_match[5]).split(' ')[1][:5])

    text_file = open("notifications//{}.txt".format( file_name ), "w+")

    text_file.write( bot_message_last_match + bot_message_next_match )
    text_file.close()

    scheduler(file_name)



def telegram_bot_send_message( bot_message ):

    BOT_TOKEN = CommonUtility.getParameterFromFile('BOT_TOKEN')
    BOT_CHATID = str(CommonUtility.getParameterFromFile('BOT_CHATID'))

    send_text = 'https://api.telegram.org/bot' + \
                                     BOT_TOKEN + \
                       '/sendMessage?chat_id=' + \
                                    BOT_CHATID + \
                  '&parse_mode=Markdown&text=' + bot_message

    requests.get(send_text)

def telegram_bot_send_photo( photo_path ):

    BOT_TOKEN = CommonUtility.getParameterFromFile('BOT_TOKEN')
    BOT_CHATID = str(CommonUtility.getParameterFromFile('BOT_CHATID'))

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    files = {}
    files["photo"] = open(photo_path, "rb")

    requests.get(url, params={"chat_id": BOT_CHATID}, files=files)

def telegram_bot_sendlog( bot_message ):

    BOT_TOKEN = CommonUtility.getParameterFromFile('BOT_TOKEN')
    BOT_CHATID = '-1001215704490'

    send_text = 'https://api.telegram.org/bot' + \
                                     BOT_TOKEN + \
                       '/sendMessage?chat_id=' + \
                                    BOT_CHATID + \
                  '&parse_mode=Markdown&text=' + bot_message

    requests.get(send_text)

