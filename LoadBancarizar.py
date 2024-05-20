import pandas as pd
import os
from sqlalchemy import create_engine

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def load_bancarizar(ruta: str):
    engine = create_engine('mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
                           ':3306/salessystem')

    bancarizar = pd.read_excel(ruta + '/importar.xlsx', sheet_name='bancarizar', date_format='%d/%m/%Y',
                               parse_dates=[2, ]
                               , na_values=' ')
    bancarizar['adquiriente'] = bancarizar['adquiriente'].str.strip().str.upper()
    bancarizar['proveedor'] = bancarizar['proveedor'].str.strip().str.upper()
    bancarizar['documento_relacionado'] = bancarizar['documento_relacionado'].str.strip().str.upper()
    bancarizar['observaciones'] = bancarizar['observaciones'].str.strip().str.upper()

    return print(bancarizar.to_sql('v_bcp', engine, if_exists='append', index=False))


load_bancarizar('C:/Users/Raknaros/Desktop/temporal')
