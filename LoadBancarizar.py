import pandas as pd
import os
from sqlalchemy import create_engine

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def load_bancarizar(ruta: str):
    engine = create_engine(
        'mysql+pymysql://admin:Giu12FF8DB*@salessystem.crkwsaygg8b2.us-east-2.rds.amazonaws.com'
        ':3306/salessystem')

    lista_bcp = []

    bancarizar = pd.read_excel(ruta + '/importar.xlsx', sheet_name='bancarizar', date_format='%d/%m/%Y',
                               parse_dates=[2, ], dtype={'observaciones': str}
                               , na_values=' ')

    str_columns = ['adquiriente', 'proveedor', 'documento_relacionado', 'observaciones']
    for column in str_columns:
        if bancarizar[column].notna().any():
            bancarizar[column] = bancarizar[column].apply(lambda x: x.strip().upper() if pd.notna(x) else x)

    ibk = bancarizar[~bancarizar['proveedor'].isin(lista_bcp)]
    bcp = bancarizar[bancarizar['proveedor'].isin(lista_bcp)]

    #if not bcp.empty:
    #    print(bcp.to_sql('v_bcp', engine, if_exists='append', index=False))
    if not ibk.empty:
        ibk = ibk.set_axis(['adquiriente', 'proveedor', 'fecha', 'importe', 'factura', 'observaciones'],
                                         axis=1)
        print(ibk.to_sql('v_ibk', engine, if_exists='append', index=False))


load_bancarizar('D:/OneDrive/facturacion')
