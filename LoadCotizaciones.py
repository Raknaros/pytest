import math

import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def load_cotizaciones(ruta: str):
    engine = create_engine('mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
                           ':3306/salessystem')

    cotizaciones = pd.read_excel(os.path.join(ruta, 'importar.xlsx'), sheet_name='cotizaciones', date_format='%d/%m/%Y',
                                 dtype={'ruc': np.int64, 'cantidad': np.int32, 'precio_unit': np.float32},
                                 parse_dates=[3, 17, 19, 21, 23, ],
                                 na_values=' ')

    cotizaciones.rename(columns={'pedido': 'cod_pedido', 'lugar_entrega': 'llegada'}, inplace=True)
    cotizaciones['cui'] = cotizaciones['cod_pedido'] + cotizaciones['cuo'].astype(str)
    cotizaciones['traslado'] = cotizaciones['traslado'].fillna(cotizaciones['emision'])
    str_columns = ['alias', 'moneda', 'descripcion', 'unid_medida', 'llegada', 'datos_adicionales',
                   'observaciones']

    cotizaciones['precio_unit'] = cotizaciones['precio_unit'].apply(lambda x: round(x, 3))
    cotizaciones['cuota1'] = cotizaciones['cuota1'].apply(lambda x: round(x, 3))
    cotizaciones['cuota2'] = cotizaciones['cuota2'].apply(lambda x: round(x, 3))
    cotizaciones['cuota3'] = cotizaciones['cuota3'].apply(lambda x: round(x, 3))
    cotizaciones['cuota4'] = cotizaciones['cuota4'].apply(lambda x: round(x, 3))

    for column in str_columns:
        if cotizaciones[column].notna().any():
            cotizaciones[column] = cotizaciones[column].apply(lambda x: x.strip().upper() if pd.notna(x) else x)

    facturas = cotizaciones[['cod_pedido', 'cuo', 'alias', 'emision', 'ruc', 'moneda', 'descripcion', 'unid_medida',
                             'cantidad', 'precio_unit', 'observaciones', 'vencimiento', 'cuota1',
                             'vencimiento2', 'cuota2', 'vencimiento3', 'cuota3', 'vencimiento4', 'cuota4']]

    def columnas_guia(group):
        guia = group.iloc[0].copy()  # Tomar el primer valor de cada columna
        total_peso = (group['cantidad'] * group['peso_articulo']).sum()  # Sumar el producto de cantidad y peso
        guia['datos_adicionales'] = 'PesoTotal: ' + str(
            math.ceil(total_peso))  # Actualizar el valor de 'peso' en el primer registro
        #guia = guia
        return guia

    remision_remitente = (cotizaciones[
                              ['cui', 'cod_pedido', 'cuo', 'alias', 'traslado', 'llegada', 'cantidad',
                               'peso_articulo', 'placa', 'conductor',
                               'datos_adicionales', 'observaciones']].groupby('cui').apply(columnas_guia,
                                                                                           include_groups=False)
                          .drop(['cantidad', 'peso_articulo'], axis=1))

    return print(facturas.to_sql('facturas', engine, if_exists='append',index=False),
                 remision_remitente.to_sql('remision_remitente', engine, if_exists='append',index=False))  #


load_cotizaciones('D:/OneDrive/facturacion')
