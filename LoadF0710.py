import pandas as pd
import os
from sqlalchemy import create_engine
import zipfile

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


# .assign(ruc=i[0:11])
def load_f0710(carpeta: str):
    set_f0710 = pd.DataFrame()
    engine = create_engine('postgresql://admindb:72656770@datawarehouse.cgvmexzrrsgs.us-east-1.rds.amazonaws.com'
                           ':5432/warehouse')
    for i in list(filter(lambda x: (x[12:16] == '0710'), os.listdir(carpeta))):
        with (zipfile.ZipFile(carpeta + '/' + i, 'r') as zipf):
            with zipf.open(i[28:36] + i[0:11] + 'pdt621_casillas.csv') as archivo_csv:
                f0621 = pd.read_csv(archivo_csv, usecols=[6, 7], header=0).T.reset_index(drop=True)

    return print(set_f0710)  # .to_sql('_9', engine, if_exists='append', index=False, schema='acc')


load_f0710('C:/Users/Raknaros/Desktop/temporal/declaraciones')
