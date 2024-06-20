from time import sleep

import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def tofacturas(proveedor: str):
    salessystem = create_engine(
        'mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
        ':3306/salessystem')

    lista_emision = pd.read_sql("SELECT * FROM lista_emitir WHERE alias = '" + proveedor + "'", salessystem)
    #emitir.style.format({'cantidad': '{:.0f}', 'precio_unit': '{:.3f}'})

    lista = pd.pivot_table(lista_emision, values=["sub_total", "igv", "total", "vencimiento", "moneda"],
                           index=['cui', 'guia', 'numero', 'emision', 'descripcion', 'unidad_medida', 'cantidad', 'p_unit'],
                           aggfunc={'sub_total': 'sum', 'igv': 'sum', 'total': 'sum', 'vencimiento': 'first',
                                    'moneda': 'first'})

    lista = lista[['sub_total', 'igv', 'total', 'vencimiento', 'moneda']]

    lista = pd.concat([
        y._append(y[['sub_total', 'igv', 'total']].sum().rename((x, '', '', '', '', '', '', 'Totales')))
        for x, y in lista.groupby(level=0)
    ])
    #Considerar agregar multihoja por proveedor
    return lista.to_excel('lista_emision.xlsx', sheet_name='emitir', float_format='%.3f')
#TODO agregar parametro rango de fechas, formatear la fecha y asegurar los 3 decimales en caso de ser necesario

tofacturas('SONICSERV')
