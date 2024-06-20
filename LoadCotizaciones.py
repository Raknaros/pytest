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
                   'observaciones']

    cotizaciones['precio_unit'] = cotizaciones['precio_unit'].apply(lambda x: round(x, 3))
    cotizaciones['cuota1'] = cotizaciones['cuota1'].apply(lambda x: round(x, 3))
    cotizaciones['cuota2'] = cotizaciones['cuota2'].apply(lambda x: round(x, 3))
    cotizaciones['cuota3'] = cotizaciones['cuota3'].apply(lambda x: round(x, 3))
    cotizaciones['cuota4'] = cotizaciones['cuota4'].apply(lambda x: round(x, 3))

    #sumar peso total de los articulos por cotizacion y colocarlo en datos adicionales de guia
    for column in str_columns:
        if cotizaciones[column].notna().any():
            cotizaciones[column] = cotizaciones[column].apply(lambda x: x.strip().upper() if pd.notna(x) else x)

    facturas = cotizaciones[['pedido', 'cuo', 'alias', 'emision', 'ruc', 'moneda', 'descripcion', 'unid_medida',
                             'cantidad', 'precio_unit', 'observaciones', 'vencimiento', 'cuota1',
                             'vencimiento2', 'cuota2', 'vencimiento3', 'cuota3', 'vencimiento4', 'cuota4']]
    #AGRUPAR COLOCANDO EL PRIMER VALOR DE CADA COLUMNA Y MULTIPLICANDO PESO POR CANTIDAD SUMANDO
    #remision_remitente = cotizaciones[
    #    ['pedido', 'cuo', 'alias', 'traslado', 'lugar_entrega', 'cantidad', 'peso_articulo', 'placa', 'conductor',
    #     'datos_adicionales', 'observaciones']]

    def agg_custom(group):
        first_values = group.iloc[0]  # Tomar el primer valor de cada columna
        total_peso = (group['cantidad'] * group['peso_articulo']).sum()  # Sumar el producto de cantidad y peso
        first_values['datos_adicionales'] = total_peso  # Actualizar el valor de 'peso' en el primer registro

        return first_values

    # Agrupar por 'cuo' y aplicar la función de agregación personalizada
    remision_remitente = cotizaciones[
        ['pedido', 'cuo', 'alias', 'traslado', 'lugar_entrega', 'cantidad', 'peso_articulo', 'placa', 'conductor',
         'datos_adicionales', 'observaciones']].groupby('cuo').apply(agg_custom)
    return print(remision_remitente)  #.to_sql('pedidos', engine, if_exists='append', index=False)


load_cotizaciones('D:/')
