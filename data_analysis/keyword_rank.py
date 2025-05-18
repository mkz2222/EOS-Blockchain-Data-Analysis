import pandas as pd
import numpy as np
import mysql.connector
import time
from datetime import datetime
from datetime import timedelta
import logging


logging.basicConfig(level=logging.INFO, filename="keyrank_1", filemode="a+",
                    format="%(asctime)-15s %(levelname)-8s %(message)s")


logging.info("#### New service start ####")


db_table = ''
db_db = ''
db_user = ''
db_pass =''
db_host =''

day_range = 7

cnx = mysql.connector.connect(
user = db_user,
password = db_pass,
host = db_host,
database = db_db)
cursor = cnx.cursor()

now = datetime.now()
now.strftime('%Y-%m-%d %H:%M:%S')
timespan = timedelta(days=day_range)
past = now - timespan

data_sql = {
    'now' : now,
    'past' : past,
}

qy_sql = ("SELECT * FROM " + db_table + " WHERE time BETWEEN %(past)s AND %(now)s")

try:

    cursor.execute(qy_sql, data_sql)
    data_rows = cursor.fetchall()


except mysql.connector.Error as err:
    logging.info("SQL connect err")
    logging.info(f"SQL error: {err.errno}")
    logging.info(f"SQL error: {err}")


df = pd.DataFrame(data_rows, columns=cursor.column_names)

cursor.close()
cnx.close()

df['keyword'] = df['keyword'].str.lower()

#remove empty keyword
df1 = df[(df['keyword'] != "")]
df1 = df[(df['keyword'] != ".")]


df_dr = df1.drop(['id','ip', 'time'], axis=1)  

df_vol = df_dr.apply(pd.value_counts) 

df_vol2 = df_vol.reset_index()

df_vol2.rename(columns={'index':'keyword', 'keyword':'vol'}, inplace = True)

df_vol2_10 = df_vol2.head(10)


#write to database

db_table_kr = ''
db_db = ''
db_user = ''
db_pass =''
db_host =''


cnx = mysql.connector.connect(
user = db_user,
password = db_pass,
host = db_host,
database = db_db)

cursor = cnx.cursor()


for i in range(len(df_vol2_10)):
    
    kw1 = df_vol2_10.loc[i, 'keyword']
    vol1 = int(df_vol2_10.loc[i, 'vol'])
    
    v_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    new_id = i + 1
    data_sql = {

        'name' : kw1,
        'value' : vol1,
        'log_time' : v_time,
        'id' : new_id,
    }

#     add_sql = ("INSERT INTO " + db_table_kr + " (name, value, log_time) "
#                "VALUES (%(name)s, %(value)s, %(log_time)s)")

#changed to name, value
    add_sql = ("UPDATE " + db_table_kr + " SET name = %(name)s, value = %(value)s, log_time = %(log_time)s "
               "WHERE id= %(id)s")

    
    try:
        cursor.execute(add_sql, data_sql)
        logging.info(f"data inserted: {kw1}")


    except mysql.connector.Error as err:

        logging.info(f"SQL error: {err.errno}")
        if err.errno == 1062:
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

            except mysql.connector.Error as err:

                if err.errno == 1062:
                    logging.info("duplicate fixed")
                    pass
                else:
                    logging.info(f"SQL error: {err}")
       
    
cursor.close()
cnx.commit() 
cnx.close()

logging.info("#### done ####")