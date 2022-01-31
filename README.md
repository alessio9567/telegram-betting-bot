# TelegramBettingBot

This is my personal project and consists of a series of Python scripts running on a Raspberry Pi Zero W and written in a pretty functional way ( a lot of functions and modules).
These scripts are scheduled every day and launched by some Shell scripts. 

The process initially gets new data from this website using Beautifoul soup for scraping:

![image](https://user-images.githubusercontent.com/45591868/136972925-095efc39-c536-4e7a-b592-7d88b44c82e0.png)

Then, data are managed in runtime through Pandas Dataframes and saved on a local Mysql database ; there are some checks that only new data are inserted into the tables while altready existing data are skipped ( incremental ETL by hashing web page )

Finally, there is a script that outputs upcoming matches satisfying certain criteria in a custom Telegram channel 
Here is a message example
![image](https://user-images.githubusercontent.com/45591868/136973105-55ef0f0c-07d0-49e3-bf7b-493cd75b1001.png)

Telegram channel link: https://t.me/joinchat/amTegTHB6cU0NWRk
Source data : www.betexplorer.com
