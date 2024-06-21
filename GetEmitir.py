from time import sleep

import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def tofacturas(proveedor: str, dias: str):
    salessystem = create_engine(
        'mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
        ':3306/salessystem')

    facturas = {}

    guias = {}

    lista_facturas = pd.read_sql("SELECT * FROM lista_facturas", con=salessystem,
                                 parse_dates=['emision', 'vencimiento', 'vencimiento2', 'vencimiento3', 'vencimiento4'])

    list_guias = pd.read_sql("SELECT * FROM lista_guias", salessystem, index_col='cui',
                             parse_dates=['traslado'])

    for i in lista_facturas['alias'].unique():
        facturas[i] = lista_facturas[lista_facturas['alias'] == i]

    with pd.ExcelWriter('PorEmitir.xlsx', engine='xlsxwriter') as writer:
        lista = pd.pivot_table(facturas, values=["sub_total", "igv", "total", "vencimiento", "moneda"],
                               index=['cui', 'guia', 'numero', 'emision', 'descripcion', 'unidad_medida', 'cantidad',
                                      'p_unit'],
                               aggfunc={'sub_total': 'sum', 'igv': 'sum', 'total': 'sum', 'vencimiento': 'first',
                                        'moneda': 'first'})

        lista = lista[['sub_total', 'igv', 'total', 'vencimiento', 'moneda']]
        lista = lista.sort_index(level='emision')
        #TODO ORDENAR POR FECHA DE EMISION (CONSIDERAR QUE ES UN INDICE)

        lista = pd.concat([
            y._append(y[['sub_total', 'igv', 'total']].sum().rename((x, guias.at[x, 'placa'], guias.at[x, 'conductor'],
                                                                     guias.at[x, 'traslado'], guias.at[x, 'llegada'],
                                                                     guias.at[x, 'datos_adicionales'], '', 'Totales')))
            for x, y in lista.groupby(level=0)
        ])
        print(lista.index.names)
        #Considerar agregar multihoja por proveedor

        #return lista.to_excel('lista_emision.xlsx', sheet_name='emitir', float_format='%.3f')


#TODO agregar parametro rango de fechas, formatear la fecha y asegurar los 3 decimales en caso de ser necesario

tofacturas('ELITE')
