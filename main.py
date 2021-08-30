import pandas as pd
import psycopg2
from datetime import date
import os

def csv_toFile():
    df = pd.read_csv("data/order_details.csv", encoding= "UTF-8", sep=",")
    
    data_atual = date.today()

    os.mkdir('data/csv/{}'.format(data_atual))

    df.to_csv('data/csv/{}/order_details.csv'.format(data_atual));

def con_sql():
    conn = psycopg2.connect(dbname="northwind", user="northwind_user", host="localhost", password="thewindisblowing")     
    return conn

def sql_toFile():
    tables = ['categories','products','suppliers','employees', 'employee_territories','territories','region','customers','customer_customer_demo','customer_demographics','orders','shippers','us_states']
    sql = "SELECT * FROM "
    data_atual = date.today()
    conn = con_sql()
    for i in tables:
        os.mkdir('data/postgres/{}/{}'.format(i,data_atual))
        sql_conn = sql + i
        dados = pd.read_sql(sql_conn, conn)
        dados.to_csv('data/postgres/{}/{}/northwind.csv'.format(i,data_atual))

sql_toFile()
 