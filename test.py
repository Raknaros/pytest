import pandas as pd
from sqlalchemy import create_engine
import mysql.connector

try:
    mydb = mysql.connector.connect(
        host="sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com",
        user="admin",
        password="Giu72656770"
    )
    print("Connection established")


except mysql.connector.Error as err:
    print("An error occurred:", err)
