import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def load_cotizaciones(ruta: str):
    engine = create_engine('mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
                           ':3306/salessystem')

    cotizaciones = pd.read_excel(ruta + '/importar.xlsx', sheet_name='cotizaciones', date_format='%d/%m/%Y',
                                 dtype={'ruc': np.int64, 'cantidad': np.int32, 'precio_unit': np.float32},
                                 parse_dates=[3, 17, 19, 21, 23, ],
                                 na_values=' ')

    str_columns = ['alias', 'moneda', 'descripcion', 'unid_medida', 'lugar_entrega', 'datos_adicionales',
                   'forma_pago', 'observacion']

    cotizaciones['precio_unit'] = cotizaciones['precio_unit'].apply(lambda x: round(x, 3))
    cotizaciones['cuota1'] = cotizaciones['cuota1'].apply(lambda x: round(x, 3))
    cotizaciones['cuota2'] = cotizaciones['cuota2'].apply(lambda x: round(x, 3))
    cotizaciones['cuota3'] = cotizaciones['cuota3'].apply(lambda x: round(x, 3))
    cotizaciones['cuota4'] = cotizaciones['cuota4'].apply(lambda x: round(x, 3))

    facturas = cotizaciones[['pedido', 'cuo', 'alias', 'emision', ' ruc', 'moneda', 'descripcion', 'unid_medida',
                             'cantidad', 'precio_unit', 'forma_pago', 'observacion', 'vencimiento', 'cuota1',
                             'vencimiento2', 'cuota2', 'vencimiento3', 'cuota3', 'vencimiento4', 'cuota4']]
    remision_remitente = cotizaciones[['pedido', 'cuo', 'alias', 'emision', 'lugar_entrega', 'placa', 'conductor',
                                       'datos_adicionales', 'observaciones']]
    #sumar peso total de los articulos por cotizacion y colocarlo en datos adicionales de guia
    for column in str_columns:
        if cotizaciones[column].notna().any():
            cotizaciones[column] = cotizaciones[column].apply(lambda x: x.strip().upper() if pd.notna(x) else x)

    return print(cotizaciones)  #.to_sql('pedidos', engine, if_exists='append', index=False)


load_cotizaciones('C:/Users/Raknaros/Desktop/temporal')
