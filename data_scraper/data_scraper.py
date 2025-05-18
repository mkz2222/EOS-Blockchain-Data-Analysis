from eosapi import Client
import pprint
import mysql.connector
import time
import requests
import json
import sys
import random
import logging


blocknum = 0

db_table = ''
db_main_table = ''
db_db = ''

db_user = ''
db_pass =''
db_host =''
db_err_table = 'error_log'


endpoints = ['https://mainnet.eoscanada.com',
             'https://api.eosnewyork.io', 
             'https://api.eossocal.io',
             'https://nodes.eos42.io', 
             'http://eos-bp.bitfinex.com:8888' 
]


c = Client(nodes = [endpoints[0]])
d = Client(nodes = [endpoints[1]])
e = Client(nodes = [endpoints[2]])
f = Client(nodes = [endpoints[3]])
g = Client(nodes = [endpoints[4]])



logging.basicConfig(level=logging.INFO, filename="logfile", filemode="a+",
                    format="%(asctime)-15s %(levelname)-8s %(message)s")


block = {}



#functions
#Get the last record from database
def get_last_sql():
    
    cnx = mysql.connector.connect(
    user = db_user,
    password = db_pass,
    host = db_host,
    database = db_db)
    cursor = cnx.cursor()

    qy_sql = ("SELECT MAX(block) FROM " + db_main_table)

    try:
        cursor.execute(qy_sql)

        row = cursor.fetchone()
        return row[0]

    except mysql.connector.Error as err:

        logging.info(err.errno)

    cursor.close()
    cnx.close()
    


#Get the last block# from blockchain
def get_last_blk(info_url):
    
    try:
        response = requests.post(info_url[0])
        block = response.json()
        return block['last_irreversible_block_num']
    except:
        try:
            response = requests.post(info_url[1])
            block = response.json()
            return block['last_irreversible_block_num']
        except:
            try: 
                response = requests.post(info_url[2])
                block = response.json()
                return block['last_irreversible_block_num']
            except:
                raise

                
#Calculate the # of blocks needs to be retrieved and assign speed based on the number.
def get_start(last_blk, last_sql):
    
    delta_blk = last_blk - last_sql
    global nstart 
    global nend
    global intv
    
    nstart = last_sql

    if delta_blk >= 1500000:

        nend = last_sql + maxspeed #maxspeed = 1,500,000
        intv = 1000

    elif 1000 <= delta_blk <= 1500000:

        nend = last_blk
        intv = 1000

    elif delta_blk < 1000:
        nend = last_blk
        intv = delta_blk
                
    elif delta_blk < 0:
                    
        logging.info("Current blk is wrong")
        raise
    




#Get data thru API
def get_account_api(x, cursor, c):

    block = c.get_block(str(x))
    

    if len(block['transactions']):

        for i in range(len(block['transactions'])):

            if 'transaction' in block['transactions'][i]['trx']:

                for x in range(len(block['transactions'][i]['trx']['transaction']['actions'])):

                    if (block['transactions'][i]['trx']['transaction']['actions'][x]['name'] == 'newaccount' and 
                        "name" in block['transactions'][i]['trx']['transaction']['actions'][x]['data'] 
                       ):

                        currentblock.append(block['transactions'][i]['trx']['transaction']['actions'][x]['data']['name'])
                        currentblock.append(block['timestamp'])
                        currentblock.append(blocknum)
                        currentblock.append(block['transactions'][i]['trx']['transaction']['actions'][x]['data']['creator'])

                        logging.info(currentblock)

                        data_sql = {

                            'name' : currentblock[0],
                            'date_created' : currentblock[1],
                            'block' : int(currentblock[2]),
                            'creator' : currentblock[3],
                            'time_created' : currentblock[1]
                        }

                        add_sql = ("INSERT INTO " + db_table + " (name, date_created, block, creator, time_created) "
                                   "VALUES (%(name)s, %(date_created)s, %(block)s, %(creator)s, %(time_created)s)")

                        try:
                            cursor.execute(add_sql, data_sql)
                            currentblock.clear()

                        except mysql.connector.Error as err:

                            logging.info(err.errno)
                            if err.errno == 1062:
                                currentblock.clear()
                                logging.info("passed")
                                pass
                            else:
                                try:

                                    logging.info("Retrying in 1 sec")
                                    time.sleep(1)

                                    cnx = mysql.connector.connect(
                                    user = db_user,
                                    password = db_pass,
                                    host = db_host,
                                    database = db_db)

                                    cursor = cnx.cursor()

                                    cursor.execute(add_sql, data_sql)
                                    logging.info("fixed")
                                    currentblock.clear()

                                except mysql.connector.Error as err:

                                    if err.errno == 1062:
                                        currentblock.clear()
                                        logging.info("duplicate fixed")
                                        pass
                                    else:
                                        currentblock.clear()
                                        logging.info(f"SQL error: {err}")

                                    

#                     else:
#                         print('No new account')
            else:
                logging.info('Bad transaction')

#     else:
#         print('No transacrion')






#Get data thru POST
def get_account_req(x, cursor, i):
        
    blk_json = {'block_num_or_id': x}

    response = requests.post(url[i], json = blk_json)
    jblock = response.json()

    if len(jblock['transactions']):

        for i in range(len(jblock['transactions'])):

            if 'transaction' in jblock['transactions'][i]['trx']:

                for x in range(len(jblock['transactions'][i]['trx']['transaction']['actions'])):

                    if (jblock['transactions'][i]['trx']['transaction']['actions'][x]['name'] == 'newaccount' and 
                        "name" in jblock['transactions'][i]['trx']['transaction']['actions'][x]['data'] 
                       ):

                        currentblock.append(jblock['transactions'][i]['trx']['transaction']['actions'][x]['data']['name'])
                        currentblock.append(jblock['timestamp'])
                        currentblock.append(blocknum)
                        currentblock.append(jblock['transactions'][i]['trx']['transaction']['actions'][x]['data']['creator'])

                        logging.info(currentblock)
                        
                        data_sql = {

                            'name' : currentblock[0],
                            'date_created' : currentblock[1],
                            'block' : int(currentblock[2]),
                            'creator' : currentblock[3],
                            'time_created' : currentblock[1]
                        }

                        add_sql = ("INSERT INTO " + db_table + " (name, date_created, block, creator, time_created) "
                                   "VALUES (%(name)s, %(date_created)s, %(block)s, %(creator)s, %(time_created)s)")

                        try:
                            cursor.execute(add_sql, data_sql)
                            currentblock.clear()

                        except mysql.connector.Error as err:
                            
                            logging.info(err.errno)
                            if err.errno == 1062:
                                currentblock.clear()
                                logging.info("passed")
                                pass

                            else:
                                try:

                                    logging.info("Retrying in 2 sec")
                                    time.sleep(2)

                                    cnx = mysql.connector.connect(
                                    user = db_user,
                                    password = db_pass,
                                    host = db_host,
                                    database = db_db)

                                    cursor = cnx.cursor()

                                    cursor.execute(add_sql, data_sql)
                                    logging.info("fixed")
                                    currentblock.clear()
                                    
                                except mysql.connector.Error as err:

                                    if err.errno == 1062:
                                        currentblock.clear()
                                        logging.info("duplicate fixed")
                                        pass
                                    else:
                                        currentblock.clear()
                                        logging.info(f"SQL error: {err}")


#                     else:
#                         print('No new account')
            else:
                logging.info('Bad transaction')

#     else:
#         print('No transacrion')






#MAIN

maxspeed = 1500000


add_url = "/v1/chain/get_block"
add_url2 = "/v1/chain/get_info"
url = []
info_url = []


for i in range(len(endpoints)):
    url.append(endpoints[i] + add_url)
    info_url.append(endpoints[i] + add_url2)


    
scount = 0
currentblock =[]


while True:
    
    if blocknum == 0:
        
        last_sql = get_last_sql()
    else:
        last_sql = blocknum
            

    last_blk = get_last_blk(info_url)

    get_start(last_blk, last_sql)
    
    logging.info("service started")
    logging.info(f"last_sql: {last_sql}")
    logging.info(f"last_blk: {last_blk}")
    logging.info(f"nstart: {nstart}")
    logging.info(f"nend: {nend}")
    logging.info(f"intv: {intv}")
    
    logging.info(f"serice #: {scount}")
    
    while nstart < nend:

            cnx = mysql.connector.connect(
            user = db_user,
            password = db_pass,
            host = db_host,
            database = db_db)

            cursor = cnx.cursor()

            rannum = random.randrange(len(endpoints))
            
            if rannum == 0:
                logging.info('using Node c')
                whichnode = "c"
            elif rannum == 1:
                logging.info('using Node d')
                whichnode = "d"
            elif rannum == 2:
                logging.info('using Node e')
                whichnode = "e"
            elif rannum == 3:
                logging.info('using Node f')
                whichnode = "f"
            elif rannum == 4:
                logging.info('using Node g')
                whichnode = "g"
            
            for x in range(nstart, nstart+intv):

                blocknum = x
                logging.info('%s - %s', whichnode, str(x))

                try:

                    #USE get_account_api
                    
                    if rannum == 0:
                        get_account_api(x, cursor, c)
                    elif rannum == 1:
                        get_account_api(x, cursor, d)
                    elif rannum == 2:
                        get_account_api(x, cursor, e)
                    elif rannum == 3:
                        get_account_api(x, cursor, f)
                    elif rannum == 4:
                        get_account_api(x, cursor, g)
                            

                except UnicodeDecodeError:
                    logging.info(f"Decode error: {sys.exc_info()[0]}")

                    try: 
                        get_account_req(x, cursor, 0)
                        logging.info("fixed Decode error")
                    except:
                        logging.info(f"Unexpected error: {sys.exc_info()}")
                        raise

                except:
                    logging.info(f"Unexpected error: {sys.exc_info()}")
                    logging.info("restarting... in 2 seconds")
                    time.sleep(2)

                    try: 
                        logging.info("Switched to BP c")
                        get_account_api(x, cursor, c)
                        logging.info("error fixed")
                    except:
                        
                        try:
                            logging.info("Switched to BP g")
                            get_account_api(x, cursor, g)
                            logging.info("error fixed")
                        except:
                            try:
                                logging.info("Switched to BP 0")
                                get_account_req(x, cursor, 0)
                                logging.info("error fixed")
                            except:
                                try:
                                    logging.info("Switched to BP f")
                                    get_account_api(x, cursor, f)
                                    logging.info("error fixed")
                                except:
                                    try:
                                        logging.info("Switched to BP 4")
                                        get_account_req(x, cursor, 4)
                                        logging.info("error fixed")
                                    except:                                
                                        #print("Unexpected error:",  sys.exc_info()[0])
                                        try:
                                            error_str = sys.exc_info()[1]
                                            logging.info(error_str)

                                            if str(error_str) == """'transactions'""":

                                                logging.info("starting reporting")

                                                cnx = mysql.connector.connect(
                                                user = db_user,
                                                password = db_pass,
                                                host = db_host,
                                                database = db_db)

                                                data_err = {
                                                    'block' : x,
                                                    'error' : str(sys.exc_info()[0]) + str(sys.exc_info()[1])
                                                }

                                                add_err = ("INSERT INTO " + db_err_table + " (block, error) "
                                                           "VALUES (%(block)s, %(error)s)")

                                                cursor = cnx.cursor()

                                                cursor.execute(add_err, data_err)
                                                logging.info("reported")
                                            pass
                                        
                                        except:
                                        
                                            logging.info(f"Unexpected error: {sys.exc_info()[0]}")
                                            logging.info(sys.exc_info()[1])
                                            logging.info(sys.exc_info()[2])

                                            #notify thru server
                                            r = requests.post('', 
                                                          data = {'notify':'error: ' + str(sys.exc_info())})
                                            raise

            cursor.close()
            cnx.close()
            logging.info('Done')
            time.sleep(2)
            nstart += intv
            
    logging.info(f"service # {scount} completed, wait 5 sec...")
    scount +=1
    time.sleep(5)

            
else:
    
    logging.info("service teminated")
    r = requests.post('', 
                      data = {'notify':'service ' + str(scount) + 'terminated'})

