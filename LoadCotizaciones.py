import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def load_cotizaciones(ruta: str):
    engine = create_engine('mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
                           ':3306/salessystem')

    cotizaciones = pd.read_excel(ruta + '/importar.xlsx', sheet_name='pedidos', date_format='%d/%m/%Y',
                            dtype={'periodo': np.int32, 'adquiriente': object, 'importe_total': np.int64, 'rubro': str,
                                   'promedio_factura': None, 'contado_credito': str,
                                   'bancariza': bool, 'punto_entrega': str, 'notas': str, 'estado': str},
                            parse_dates=[0, ],
                            na_values=' ', false_values=['no', 'NO', 'No'], true_values=['si', 'Si', 'SI'])
    cotizaciones['rubro'] = cotizaciones['rubro'].str.strip().str.upper()
    cotizaciones['contado_credito'] = cotizaciones['contado_credito'].str.strip().str.upper()
    cotizaciones['punto_entrega'] = cotizaciones['punto_entrega'].str.strip().str.upper()
    cotizaciones['notas'] = cotizaciones['notas'].str.strip().str.upper()
    cotizaciones['estado'] = cotizaciones['estado'].str.strip().str.upper()

    return print(cotizaciones.to_sql('pedidos', engine, if_exists='append', index=False))  #


load_cotizaciones('C:/Users/Raknaros/Desktop/temporal')
