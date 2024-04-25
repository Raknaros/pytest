import pandas as pd
import os
from sqlalchemy import create_engine
import psycopg2

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def load_iqbf(carpeta):
    set_iqbf = pd.DataFrame()
    engine = create_engine('postgresql://admindb:72656770@datawarehouse.cgvmexzrrsgs.us-east-1.rds.amazonaws.com'
                           ':5432/warehouse')
    for i in os.listdir(carpeta):
        iqbf = pd.read_excel('C:\\Users\\Raknaros\\Desktop\\temporal\\' + i, sheet_name=[0, 3], skiprows=5,
                             skipfooter=5, usecols='B:AG', date_format='%d/%m/%Y', parse_dates=[2, ], header=None,
                             na_values=' ')
        set_iqbf = set_iqbf._append([iqbf[0].assign(ruc=i[5:16]), iqbf[3].assign(ruc=i[5:16])], ignore_index=True)
    set_iqbf.iloc[:, 3:27] = set_iqbf.iloc[:, 3:27].astype(object, errors='ignore')