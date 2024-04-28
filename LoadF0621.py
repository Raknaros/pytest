import pandas as pd
import os
from sqlalchemy import create_engine
import psycopg2
import zipfile

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


# .assign(ruc=i[0:11])
def load_f0621(carpeta: str):
    set_f0621 = pd.DataFrame()
    engine = create_engine('postgresql://admindb:72656770@datawarehouse.cgvmexzrrsgs.us-east-1.rds.amazonaws.com'
                           ':5432/warehouse')
    for i in list(filter(lambda x: (x[12:16] == '0621'), os.listdir(carpeta))):
        with (zipfile.ZipFile(carpeta + '/' + i, 'r') as zipf):
            with zipf.open(i[28:36] + i[0:11] + 'pdt621_casillas.csv') as archivo_csv:
                f0621 = pd.read_csv(archivo_csv, usecols=[6, 7], header=0).T.reset_index(drop=True)
                f0621.columns = f0621.iloc[0]
                f0621 = f0621[1:].add_prefix('_')#.drop(
                    #['_5', '_46', '_56', '_58', '_509', '_510', '_511', '_861', '_886', '_887', '_888', '_889',
                     #'_895'], axis=1).assign(numero_orden=i[17:27])
                f0621.rename(columns={'_2': 'ruc', '_7': 'periodo_tributario', '_13': 'fecha_presentacion'},
                             inplace=True)
        set_f0621 = set_f0621._append(f0621, ignore_index=True)
        #pendiente probar metodo merge
    return print(set_f0621)#.to_sql('_9', engine, if_exists='append', index=False, schema='acc')


load_f0621('C:/Users/Raknaros/Desktop/temporal/declaraciones')
