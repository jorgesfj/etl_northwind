import pandas as pd
import psycopg2
from datetime import date
import os
import csv

def csv_toFile():
    df = pd.read_csv("data/order_details.csv", encoding= "UTF-8", sep=",")
    
    data_atual = date.today()

    os.mkdir('data/csv/{}'.format(data_atual))

    df.to_csv('data/csv/{}/order_details.csv'.format(data_atual));

def con_sql(name):
    conn = psycopg2.connect(dbname= name, user="postgres", host="localhost", password="58545256jj")     
    return conn

def sql_toFile():
    tables = ['categories','products','suppliers','employees', 'employee_territories','territories','region','customers','customer_customer_demo','customer_demographics','orders','shippers','us_states']
    sql = "SELECT * FROM "
    data_atual = date.today()
    conn = con_sql("northwind")
    for i in tables:
        os.mkdir('data/postgres/{}/{}'.format(i,data_atual))
        sql_conn = sql + i
        dados = pd.read_sql(sql_conn, conn)
        dados.to_csv('data/postgres/{}/{}/northwind.csv'.format(i,data_atual))
    conn.close()

def create_tables():
    data_atual = date.today()
    lista = []
    lista2 = []

    order=[]
    order2=[]
    #Abrindo o csv local
    with open("data/csv/"+str(data_atual)+"/order_details.csv","r") as arquivo:
        texto = arquivo.readlines()
    for linha in texto:
        lista.append(linha)
    for i in range(len(lista)):
        lista2.append(lista[i].split(","))

    with open("data/postgres/orders/"+str(data_atual)+"/northwind.csv", "r", encoding="utf-8") as arquivo:
        texto = arquivo.readlines()
    for linha in texto:
        order.append(linha)
    for i in range(len(order)):
        order2.append(order[i].split(","))

    conn = con_sql("northwind_output")
    cur = conn.cursor()
    #sql_order = "CREATE TABLE orders (order_id smallint NOT NULL,customer_id bpchar,employee_id smallint,order_date date,required_date date,shipped_date date,ship_via smallint,freight real,ship_name text,ship_address text,ship_city text,ship_region text,ship_postal_code text,ship_country text)"
    #sql_order_details = "CREATE TABLE order_details (order_id smallint NOT NULL,product_id smallint NOT NULL,unit_price real ,quantity smallint, discount real)"
    #cur.execute(sql_order)
    for i in range(1,len(lista2)):
        cur.execute("INSERT INTO order_details VALUES({},{},{},{},{})".format(lista2[i][1],lista2[i][2],lista2[i][3],lista2[i][4],lista2[i][5]))
    
    for i in range(1, len(order2)):
        cur.execute("INSERT INTO orders VALUES ({}, '{}',{}, '{}', '{}', '{}', {}, {}, '{}', '{}', '{}', '{}', '{}', '{}')".format(order2[i][1],order2[i][2],order2[i][3],order2[i][4],order2[i][5],order2[i][6],order2[i][7],order2[i][8],order2[i][9],order2[i][10],order2[i][11],order2[i][12],order2[i][13],order2[i][14]))
    #cur.execute(sql_order_details)
    conn.commit()

create_tables()