from eosapi import Client
import mysql.connector
import time
import requests
import json
import logging
from datetime import datetime
import random

#config


eos_contracts = ['eosnameswaps',
				'exchangename',
				]


endpoints = [
             'https://api.eosnewyork.io:443', 
             'https://nodes.eos42.io',
             'https://hapi.eosrio.io',
             'https://api-mainnet.eosgravity.com/',
             'https://api.eosrio.io',
]


#DC1
db_table = ''
db_table3 = ''
db_db = ''
db_user = ''
db_pass =''
db_host =''

#godaddy
db_table2 = ''
db_db2 = ''
db_user2 = ''
db_pass2 =''
db_host2 =''


logging.basicConfig(level=logging.INFO, filename="acct4sale", filemode="a+",
                    format="%(asctime)-15s %(levelname)-8s %(message)s")



c = Client(nodes = [endpoints[0]])
d = Client(nodes = [endpoints[1]])
e = Client(nodes = [endpoints[2]])
f = Client(nodes = [endpoints[3]])
g = Client(nodes = [endpoints[4]])


# add_url = "/v1/chain/get_block"
# add_url2 = "/v1/chain/get_info"
# add_url3 = "/v1/chain/get_table_rows"
# add_url4 = "/v1/history/get_actions"


# url = []
# info_url = []
# tab_url = []
# act_url =[]


# for i in range(len(endpoints)):
#     url.append(endpoints[i] + add_url)
#     info_url.append(endpoints[i] + add_url2)
#     tab_url.append(endpoints[i] + add_url3)
#     act_url.append(endpoints[i] + add_url4)




def get_row_from_contract (si, contract_name, acct_col, table_key, table_name, lower_b, upper_b, limit, cnx, cnx2, cursor, cursor2):
    
    global last_acct
    global goon
    
    goon = 0

    if si == 1:
        atns = c.get_table_rows(True, contract_name, contract_name,table_name, table_key, lower_b, upper_b, limit)
    elif si ==2:
        atns = d.get_table_rows(True, contract_name, contract_name,table_name, table_key, lower_b, upper_b, limit)
    elif si ==3:
        atns = e.get_table_rows(True, contract_name, contract_name,table_name, table_key, lower_b, upper_b, limit)
    elif si ==4:
        atns = f.get_table_rows(True, contract_name, contract_name,table_name, table_key, lower_b, upper_b, limit)
    
    
    logging.info(f"legnth: {len(atns['rows'])}")
    
    for i in range(len(atns['rows'])):
        
        a4sale.append(atns['rows'][i][acct_col[0]])
        a4sale.append(atns['rows'][i][acct_col[1]])

        
        #get id from testdj
        qy_data ={'name' : a4sale[0],
                 'group6' : 's',}
        
        qy_sql = ("SELECT id FROM " + db_table + " WHERE name= %(name)s ")
        
#         print(a4sale[0], a4sale[1])
        
        try:
            cursor.execute(qy_sql, qy_data)
            row = cursor.fetchone()
        
            #exists in testdj
            if row:
                
                qy3_data ={
                    'acct_name' : a4sale[0],
                    'acct_info1': a4sale[1], 
                    'acct_id' : row[0],
                    'acct_info2' : acct_col[2],
                    'comm_num' : 0,
                    'group6' : 's',
                }
            
                #update group6 
                qy4_sql = ("UPDATE " + db_table + " SET group6 = %(group6)s " 
                           "WHERE name = %(acct_name)s")
                
                try:
                    cursor.execute(qy4_sql, qy3_data)
                    # logging.info('group6 updated')

                except mysql.connector.Error as err:

                    try:
                        # logging.info("Retrying in 1 sec")
                        time.sleep(1)

                        cnx = mysql.connector.connect(
                        user = db_user,
                        password = db_pass,
                        host = db_host,
                        database = db_db)

                        cursor = cnx.cursor()
                        cursor.execute(qy4_sql, qy3_data)
                        # logging.info("fixed")

                    except mysql.connector.Error as err:

                        if err.errno == 1062:
                            # logging.info("duplicate passed")
                            pass
                        else:
                            logging.info(f"SQL error: {err}")
                                
                                
            
                #insert to acct_info
                
                if acct_col[2] == 's1': #exchangename
                
                    qy3_sql = ("INSERT INTO " + db_table3 + " (acct_name, comm_num, acct_info1, acct_info2, acct_id) "  
                    "VALUES (%(acct_name)s, %(comm_num)s, %(acct_info1)s, %(acct_info2)s, %(acct_id)s) "  
                    " ON DUPLICATE KEY UPDATE acct_info1 = %(acct_info1)s, acct_info2 = %(acct_info2)s")
                    
                elif acct_col[2] == 's2': #eosnameswaps
                    
                    qy3_sql = ("INSERT INTO " + db_table3 + " (acct_name, comm_num, acct_info1, acct_info3, acct_id) "  
                    "VALUES (%(acct_name)s, %(comm_num)s, %(acct_info1)s, %(acct_info2)s, %(acct_id)s) "  
                    " ON DUPLICATE KEY UPDATE acct_info1 = %(acct_info1)s, acct_info3 = %(acct_info2)s")

                try:
                    cursor.execute(qy3_sql, qy3_data)
                    qy3_data.clear()
                    # logging.info('data inserted')

                except mysql.connector.Error as err:

                    logging.info(err.errno)
                    if err.errno == 1062:
                        # logging.info("passed")
                        pass

                    else:
                        try:
                            # logging.info("Retrying in 1 sec")
                            time.sleep(1)

                            cnx = mysql.connector.connect(
                            user = db_user,
                            password = db_pass,
                            host = db_host,
                            database = db_db)

                            cursor = cnx.cursor()
                            cursor.execute(qy3_sql, qy3_data)
                            
                            qy3_data.clear()
                            # logging.info("fixed")

                        except mysql.connector.Error as err:

                            if err.errno == 1062:
                                # logging.info("duplicate passed")
                                qy3_data.clear()
                                pass
                            else:
                                logging.info(f"SQL error: {err}")
                                qy3_data.clear()

                                
            #doesn't exist in testdj
            else:
                logging.info(f"not existed : {a4sale[0]}")

                qy2_sql = "INSERT INTO " + db_table2 + " (name) " "VALUE (%(name)s)"  

                try:
                    cursor2.execute(qy2_sql, qy_data)
                    # logging.info('missing data inserted')

                except mysql.connector.Error as err:

                    # logging.info(err.errno)
                    if err.errno == 1062:
                        # logging.info("passed")
                        pass

                    else:
                        try:

                            # logging.info("Retrying in 1 sec")
                            time.sleep(1)

                            cnx2 = mysql.connector.connect(
                            user = db_user2,
                            password = db_pass2,
                            host = db_host2,
                            database = db_db2)

                            cursor2 = cnx2.cursor()

                            cursor2.execute(qy2_sql, qy_data)
                            # logging.info("fixed")

                        except mysql.connector.Error as err:

                            if err.errno == 1062:
                                # logging.info("duplicate fixed")
                                pass
                            else:
                                logging.info(f"SQL error: {err}")


        except mysql.connector.Error as err:
            logging.info(err)
        
        qy_data.clear()
        a4sale.clear()
        
        
    #see if it is the last
    
    if atns['more']:
        last_row = len(atns['rows']) - 1
        last_acct = atns['rows'][last_row]['owner']
        goon = 1
        logging.info("there are more")
        # logging.info(f"last account : {last_acct}")

        
    else:
        last_acct =''
        goon = 0
        

        
        
        
def clean_4sale(cursor):
    
    update_sql = ("UPDATE " + db_table3 + " SET acct_info1 = '', acct_info2 = '', acct_info3='' "
                   "WHERE acct_info2 != '' or acct_info3 != '' ")

    cursor.execute(update_sql)
    logging.info('sale records cleared')

    
    
    
def clean_group6(cursor):

    update_sql = ("UPDATE " + db_table + " SET group6 = '' "
                   "WHERE group6 = 's' ")

    cursor.execute(update_sql)
    logging.info('group6 cleared')
    
    





#MAIN

cnx = mysql.connector.connect(
    user = db_user,
    password = db_pass,
    host = db_host,
    database = db_db,
    autocommit=True
)

cursor = cnx.cursor()

try:
    clean_4sale(cursor)
    logging.info("cleaned 4sale")
    
except mysql.connector.Error as err:   
    
    # logging.info(err.errno)
    try:
        # logging.info("Retry cleaning...")

        cnx = mysql.connector.connect(
        user = db_user,
        password = db_pass,
        host = db_host,
        database = db_db,
        autocommit=True)

        cursor = cnx.cursor()

        clean_4sale(cursor)
        logging.info("fixed")

    except mysql.connector.Error as err:

        logging.info(f"cleaning error: {err}")

        
try:
    clean_group6(cursor)
    logging.info("cleaned group6")

except mysql.connector.Error as err:   
    
    logging.info(err.errno)

    try:
        # logging.info("Retry cleaning...")

        cnx = mysql.connector.connect(
        user = db_user,
        password = db_pass,
        host = db_host,
        database = db_db,
        autocommit=True)

        cursor = cnx.cursor()

        clean_4sale(cursor)
        logging.info("fixed")

    except mysql.connector.Error as err:

        logging.info(f"cleaning error: {err}")

cursor.close()
cnx.close()





a4sale =[]
acct_col = []


cnx = mysql.connector.connect(
    user = db_user,
    password = db_pass,
    host = db_host,
    database = db_db,
    autocommit=True
)

cnx2 = mysql.connector.connect(
    user = db_user2,
    password = db_pass2,
    host = db_host2,
    database = db_db2,
    autocommit=True
)

cursor = cnx.cursor()
cursor2 = cnx2.cursor()
    
# now = datetime.now()
# if now.hour < 14:
# 	contract_name = eos_contracts[0]
# else:
# 	contract_name = eos_contracts[1]
    
contract_name = eos_contracts[0]

if contract_name == 'exchangename': 
    
    acct_col.clear()
    acct_col.append('owner')
    acct_col.append('price')
    acct_col.append('s1')
    table_name = 'account'
    
elif contract_name == 'eosnameswaps':

    acct_col.clear()
    acct_col.append('account4sale')
    acct_col.append('saleprice')
    acct_col.append('s2')
    table_name = 'accounts'

    
table_key = ''

lower_b = ''
upper_b = ''
limit = 400

si = random.randrange(len(endpoints)) + 1

logging.info(f"Using endpoint: {endpoints[si-1]}")

# si = 2


get_row_from_contract (si, contract_name, acct_col, table_key, table_name, lower_b, upper_b, limit, cnx, cnx2, cursor, cursor2)


while goon > 0:
    
    logging.info("wait for 5s .....")
    time.sleep(5)
    
    lower_b = last_acct
    limit = 500
    
    get_row_from_contract (si, contract_name, acct_col, table_key, table_name, lower_b, upper_b, limit, cnx, cnx2, cursor, cursor2)
    

logging.info("finished")


cursor.close()
cursor2.close()
cnx.close()
cnx2.close()