# -*- coding: utf-8 -*-
"""
Created on Mon Apr 30 18:28:52 2018

@author: aurelien
"""

import random
import pymysql
import boto3
import json
from datetime import datetime
import calendar
import random
import time
from kiner.producer import KinesisProducer
from IPython.display import clear_output
from time import sleep    
import sys


NOMBREMESSAGEPARSECONDE = 10
NOMBREMESSAGETOTAL = 10000

# Connect to the database
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='',
                             db='memoirem2',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
try:

    print("Connexion a la base MySQL pour récupération du contexte")
    with connection.cursor() as cursor:
        print("Récupération des utilisateurs")
        # chargement des users
        sql = "SELECT * FROM user"
        cursor.execute(sql)
        user = cursor.fetchall()
        userSize = len(user)
        
        print("Récupération des pages")        
        #chargement des pages    
        sql = "SELECT * FROM event_type_full_unique"
        cursor.execute(sql)
        event_type = cursor.fetchall()
        EventSize = len(event_type)
 
        print("Récupération des offres")       
         #chargement des offres    
        sql = "SELECT * FROM offer"
        cursor.execute(sql)
        offer = cursor.fetchall()
        OfferSize = len(offer)       
        
        #print(user[1])
        #print(event_type[1])
        #print(offer[1])
        print("Récupération finie ")

    
except ValueError:
    print("Erreur with the DB")
finally:
    print("fermeture de la connexion MySQL")
    connection.close()
    

sys.path.append('C:\\Users\\aurelien\\Anaconda3\\Lib\\site-packages')

client = boto3.client(
    'kinesis'

)
print("Connexion à Amazon Kinesis")

p = KinesisProducer('MemoireM2', batch_size=500, max_retries=5, threads=10)
kinesis_client = boto3.client('kinesis', region_name="eu-west-1")

print("Début de l'envoi des données")

for i  in range(1,NOMBREMESSAGETOTAL):
    idUser = random.randint(1,userSize-1)
    idEvent = random.randint(1,EventSize-1)
    if(random.randint(0,100)>10):
        idOffer = random.randint(1,OfferSize-1)
        put_msg = str("{0};{1};{2};{3};{4};{5};{6};{7};{8};{9};{10};{11}".format(str(user[idUser]["adresse_ip_user"]),str(calendar.timegm(datetime.utcnow().timetuple())),str(user[idUser]["type_device"]),str(user[idUser]["navigateur"]),str(user[idUser]["OS"]),str(event_type[idEvent]["event_name"]),str(event_type[idEvent]["merchant_id"]),str(event_type[idEvent]["merchant_name"]),str(event_type[idEvent]["category_id"]),str(offer[idOffer]["id_offer"]),str(offer[idOffer]["value_offer"]),str(offer[idOffer]["typer_offer"])))

    else:
        put_msg = str("{0};{1};{2};{3};{4};{5};{6};{7};{8};;;".format(str(user[idUser]["adresse_ip_user"]),str(calendar.timegm(datetime.utcnow().timetuple())),str(user[idUser]["type_device"]),str(user[idUser]["navigateur"]),str(user[idUser]["OS"]),str(event_type[idEvent]["event_name"]),str(event_type[idEvent]["merchant_id"]),str(event_type[idEvent]["merchant_name"]),str(event_type[idEvent]["category_id"])))

    p.put_record(put_msg)
    sleep(1/NOMBREMESSAGEPARSECONDE)
    if(i%10 == 0):
        clear_output()
        print('message send : '+str(i))
        #print(put_msg)
print("Fermeture de la connexion Kinesis")
p.close()