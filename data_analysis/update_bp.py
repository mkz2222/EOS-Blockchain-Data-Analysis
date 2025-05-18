from eosapi import Client
import mysql.connector
import time
import requests
import json
import logging


#godaddy
db_table = ''
db_db = ''
db_user = ''
db_pass =''
db_host =''


#DC1
db2_table = ''
db2_db = ''
db2_user = ''
db2_pass =''
db2_host =''


logging.basicConfig(level=logging.INFO, filename="bp_1", filemode="a+",
                    format="%(asctime)-15s %(levelname)-8s %(message)s")


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



add_url = "/v1/chain/get_block"
add_url2 = "/v1/chain/get_producers"
url = []
bp_url = []


for i in range(len(endpoints)):
    url.append(endpoints[i] + add_url)
    bp_url.append(endpoints[i] + add_url2)


logging.info("#### new update ####")



#get BP list
bp_qty = 600
bp_list = []
blk_json = {"limit":bp_qty, "json":"true", }

response = requests.post(bp_url[1], json = blk_json)
result_json = response.json()

logging.info(len(result_json['rows']))

for i in range(len(result_json['rows'])):
    # print(result_json['rows'][i]['owner'])
    bp_list.append(result_json['rows'][i]['owner'])
    


##############clear current BP
def clear_bp(cursor):
    
    data_sql = {
        'group_b4' : 'BP',
        'group_now' : '',
    }

    update_sql = ("UPDATE " + db_table + " SET group3 = %(group_now)s "
                   "WHERE group3 = %(group_b4)s")


   
    cursor.execute(update_sql, data_sql)
    logging.info('group cleared')
    

cnx = mysql.connector.connect(
    user = db_user,
    password = db_pass,
    host = db_host,
    database = db_db,
    autocommit=True
)
cursor = cnx.cursor()
    

try:
    clear_bp(cursor)
    
except mysql.connector.Error as err:   
    
    logging.info(err.errno)

    try:
        logging.info("Retrying")

        cnx = mysql.connector.connect(
        user = db_user,
        password = db_pass,
        host = db_host,
        database = db_db,
        autocommit=True)

        cursor = cnx.cursor()

        clear_bp(cursor)
        logging.info("fixed")

    except mysql.connector.Error as err:

    	logging.info(f"SQL error: {err}")


cursor.close()
cnx.close()
logging.info("DB1 BP cleaned")



##########clear BP 2
cnx2 = mysql.connector.connect(
    user = db2_user,
    password = db2_pass,
    host = db2_host,
    database = db2_db,
    autocommit=True
)
cursor2 = cnx2.cursor()
    

try:
    clear_bp(cursor2)
    
except mysql.connector.Error as err:   
    
    logging.info(err.errno)

    try:
        logging.info("Retrying")

        cnx2 = mysql.connector.connect(
        user = db2_user,
        password = db2_pass,
        host = db2_host,
        database = db2_db,
        autocommit=True)

        cursor2 = cnx2.cursor()

        clear_bp(cursor2)
        logging.info("fixed")

    except mysql.connector.Error as err:

    	logging.info(f"SQL error: {err}")


cursor2.close()
cnx2.close()
logging.info("DB2 BP cleaned")



########## update group 1

cnx = mysql.connector.connect(
    user = db_user,
    password = db_pass,
    host = db_host,
    database = db_db,
    autocommit=True
)

cursor = cnx.cursor()

for i in range(len(bp_list)):

    update_sql = ("UPDATE " + db_table + " SET group3 = %(group)s "
               "WHERE name= %(name)s")

    data_sql = {
        'name' : bp_list[i],
        'group' : 'BP',
    }

    try: 
        cursor.execute(update_sql, data_sql)
    except mysql.connector.Error as err:             
        logging.info(err.errno)
        
        try:
            logging.info("Retrying")

            cnx = mysql.connector.connect(
            user = db_user,
            password = db_pass,
            host = db_host,
            database = db_db,
            autocommit=True,
            )

            cursor = cnx.cursor()

            cursor.execute(update_sql, data_sql)
            logging.info("fixed")

        except mysql.connector.Error as err:

            logging.info(f"SQL error: {err}")

                    
cursor.close()
cnx.close()
logging.info("DB1 BP updated")





########## update group 2

cnx2 = mysql.connector.connect(
    user = db2_user,
    password = db2_pass,
    host = db2_host,
    database = db2_db,
    autocommit=True
)

cursor2 = cnx2.cursor()

for i in range(len(bp_list)):

    update_sql = ("UPDATE " + db_table + " SET group3 = %(group)s "
               "WHERE name= %(name)s")

    data_sql = {
        'name' : bp_list[i],
        'group' : 'BP',
    }

    try: 
        cursor2.execute(update_sql, data_sql)
    except mysql.connector.Error as err:             
        logging.info(err.errno)
        
        try:
            logging.info("Retrying")

            cnx2 = mysql.connector.connect(
            user = db2_user,
            password = db2_pass,
            host = db2_host,
            database = db2_db,
            autocommit=True,
            )

            cursor2 = cnx2.cursor()

            cursor2.execute(update_sql, data_sql)
            logging.info("fixed")

        except mysql.connector.Error as err:

            logging.info(f"SQL error: {err}")

                    
cursor2.close()
cnx2.close()
logging.info("DB2 BP updated")