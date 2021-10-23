
import CommonUtility
import requests
import sys

BOT_TOKEN = '1310003274:AAE6FmLQVlYWrerylGfrxN1CRhhQYekjLX4'
BOT_CHATID = str(CommonUtility.getParameterFromFile('BOT_CHATID'))

if __name__ == '__main__':
    
    with open("notifications//{}".format( sys.argv[1] ), "r" ) as bot_message_file:
        bot_message = bot_message_file.read()
    
    send_text = 'https://api.telegram.org/bot' + \
                                     BOT_TOKEN + \
                       '/sendMessage?chat_id=' + \
                                     BOT_CHATID + \
                  '&parse_mode=Markdown&text=' + \
                                     '\U0001F6A8 \U0001F6A8 \U0001F6A8 \U0001F6A8 \U0001F6A8 \U0001F6A8 \U0001F6A8  \n ' + \
                                     bot_message

    requests.get(send_text)
