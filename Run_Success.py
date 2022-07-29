import mysql.connector as msql
from mysql.connector import Error
import pandas as pd
def connector():
 empdata = pd.read_csv('StudentDetails\StudentDetails.csv', index_col=False, delimiter = ',')
 print(empdata.head())





 #import mysql.connector as msql
 #from mysql.connector import Error
 try:
    conn = msql.connect(host='localhost', user='root',  
                        password='Tameem@786')#give ur username, password
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE newdata")
        print("Database is created")
 except Error as e:
    print("Error while connecting to MySQL", e)





#import mysql.connector as msql
#from mysql.connector import Error

 try:
    conn = msql.connect(host='localhost', database='newdata', user='root', password='Tameem@786')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        cursor.execute('DROP TABLE IF EXISTS employee_data;')
        print('Creating table....')
        # in the below line please pass the create table statement which you want #to create
        cursor.execute("CREATE TABLE employee_data(SERIAL_NO varchar(255),ID varchar(255),NAME varchar(255))")
        # loop through the data frame
        for i, row in empdata.iterrows():
            # here %S means string values
            value = (row[0], row[2], row[4])
            sql = "INSERT INTO bigdata.employee_data VALUES (%s,%s,%s)"
            cursor.execute(sql, tuple(value))
            print("Record inserted")
            # the connection is not auto committed by default, so we must commit to save our changes
            conn.commit()
 except Error as e:
    print("Error while connecting to MySQL", e)
connector()
#pip install mysql-connector-python





