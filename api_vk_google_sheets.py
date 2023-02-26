import os
import requests 
from pandas import json_normalize
import pandas as pd
from datetime import datetime
from datetime import *
import httplib2 
from oauth2client.service_account import ServiceAccountCredentials
import apiclient.discovery	

# вызываем переменные окружения
TOKEN_USER =
VERSION = 
DOMAIN = 

# через api vk вызываем статистику постов
response = requests.get('https://api.vk.com/method/wall.get',
params={'access_token': TOKEN_USER,
        'v': VERSION,
        'domain': DOMAIN,
        'count': 10,
        'filter': str('owner')})

data = response.json()['response']['items']


# считаем сколько фото у поста, заводи все в df
id = []
photo = []

for post in data:
        id.append(post['id'])
        try:
                photo.append(len(post['attachments']))
        except:
                photo.append(0)

df_photo = pd.DataFrame(
    {'id': id,
     'photo.count': photo,
    })



# вытаскиваем нужные нам столбцы и переводим формат даты
df = json_normalize(data)
df = df[['id','date','comments.count','likes.count','reposts.count','reposts.wall_count','reposts.mail_count','views.count','text']]

df['date']= [datetime.fromtimestamp(df['date'][i]) for i in range(len(df['date']))]


# для каждого поста вытаскиваем дополнительную статистику
post_id = ','.join(df['id'].astype("str"))
response = requests.get('https://api.vk.com/method/stats.getPostReach',
params={'access_token': TOKEN_USER,
        'v': VERSION,
        'owner_id': -84884766,
        'post_ids': post_id})

data = response.json()['response']
df_stat_post = json_normalize(data)


# объединяем все df cо всеми статистиками и количествам фото
df_final = df.merge(df_stat_post, how='left', left_on='id', right_on="post_id")
df_final = df_final.merge(df_photo, how='left', left_on='id', right_on="id")
df_final.drop(columns='post_id',inplace=True)



# добавляем дополнительные столбцы с временем
df_final['date_time_report'] = datetime.now()
df_final['date_report'] = date.today()
df_final['year'] = df_final['date_time_report'].dt.year
df_final['month'] = df_final['date_time_report'].dt.month
df_final['day'] = df_final['date_time_report'].dt.day
df_final['hour'] = df_final['date_time_report'].dt.hour
df_final['minute'] = df_final['date_time_report'].dt.minute
df_final[['date','date_report','date_time_report']] = df_final[['date','date_report','date_time_report']].astype('str')

# сохраняем все значения
data_list = df_final.values.tolist()


# подключаемся к гугл таблице
CREDENTIALS_FILE = ''  # Имя файла с закрытым ключом, вы должны подставить свое

# Читаем ключи из файла
credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])

httpAuth = credentials.authorize(httplib2.Http()) # Авторизуемся в системе
service = apiclient.discovery.build('sheets', 'v4', http = httpAuth) # Выбираем работу с таблицами и 4 версию API 
spreadsheetId = '' #id листа

# находим последнию строку заполненную
response = service.spreadsheets().values().get(spreadsheetId = spreadsheetId,range="Лист номер один!A1:A").execute()

# последние 10 строк заполняем
number_sheet = "Лист номер один!A" + str(len(response['values'])+1) + ':AA' + str(len(response['values'])+10)


# создаем запрос и вставляем туда данные
data_vk = {
    "valueInputOption": "USER_ENTERED", # Данные воспринимаются, как вводимые пользователем (считается значение формул)
    "data": [
        {"range": "",
         "majorDimension": "ROWS",     # Сначала заполнять строки, затем столбцы
         "values": ''}
    ]
}

data_vk['data'][0]['range'] = number_sheet
data_vk['data'][0]['values'] = data_list

# выполняем запрос
results = service.spreadsheets().values().batchUpdate(spreadsheetId = spreadsheetId, body = data_vk).execute()