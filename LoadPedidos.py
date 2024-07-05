import pandas as pd
from datetime import date
from time import sleep
from sqlalchemy import create_engine
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def load_pedidos(ruta: str):
    engine = create_engine('mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
                           ':3306/salessystem')

    pedidos = pd.read_excel(ruta + '/importar.xlsx', sheet_name='pedidos', date_format='%d/%m/%Y',
                            dtype={'periodo': np.int32, 'adquiriente': object, 'importe_total': np.int64, 'rubro': str,
                                   'promedio_factura': None, 'contado_credito': str,
                                   'bancariza': bool, 'punto_entrega': str, 'notas': str, 'estado': str},
                            parse_dates=[0, ],
                            na_values=' ', false_values=['no', 'NO', 'No'], true_values=['si', 'Si', 'SI'])
    #pedidos.astype(dtype={'c': int, 'd': np.int64, 'f': np.int32}, copy=False, errors='raise')

    str_columns = ['rubro', 'contado_credito', 'punto_entrega', 'notas', 'estado']
    for column in str_columns:
        if pedidos[column].notna().any():
            pedidos[column] = pedidos[column].apply(lambda x: x.strip().upper() if pd.notna(x) else x)

    #observacion, si en algun campo de numero va algun espacio verificar como se parsea por el read_excel y corregir para este caso y otros
    #

    return print(pedidos.to_sql('pedidos', engine, if_exists='append', index=False))  #


load_pedidos('D:/OneDrive/facturacion')
