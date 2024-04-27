import pandas as pd
import os
from sqlalchemy import create_engine
import psycopg2
import zipfile

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def load_f0621(carpeta: str):
    set_f0621 = pd.Dataframe()
    engine = create_engine('postgresql://admindb:72656770@datawarehouse.cgvmexzrrsgs.us-east-1.rds.amazonaws.com'
                           ':5432/warehouse')
    for i in list(filter(lambda x: (x[12:16] == '0621'), os.listdir(carpeta))):
        with zipfile.ZipFile(carpeta+'\\' + i, 'r') as zipf:
            with zipf.open(i[34:42]+i[0:11]+'pdt621_casillas.csv') as archivo_csv:
                f0621 = pd.read_csv(archivo_csv, sep=',')
        set_f0621 = set_f0621._append(f0621[0].assign(ruc=i[5:16]), ignore_index=True)




my_list = [12, 65, 54, 39, 102, 339, 221, 50, 70, ]
my_list2 = ['ba', 'ca', 'da', 'ea', 'jd', 'kd', ]
# use anonymous function to filter and comparing
# if divisible or not
result = list(filter(lambda x: (x[1] == 'a'), my_list2))

print(result)
