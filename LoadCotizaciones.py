import math

import pandas as pd
import os
from pathlib import Path

from openpyxl.reader.excel import load_workbook
from sqlalchemy import create_engine
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('future.no_silent_downcasting', True)


def load_cotizaciones(ruta: str):
    # Cargar engine de conexion de salessystem database
    engine = create_engine('mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
                           ':3306/salessystem')

    # Determinar ruta del libro
    workbook_path = Path(ruta)
    # Cargar libro excel
    workbook = load_workbook(workbook_path)

    # Crear dataframe Cotizaciones
    cotizaciones = pd.DataFrame(
        columns=['cuo', 'alias', 'emision', 'descripcion', 'cantidad', 'precio_unit', 'total', 'peso_articulo',
                 'peso_total', 'observaciones', 'vencimiento', 'cuota1', 'vencimiento2', 'cuota2', 'vencimiento3',
                 'cuota3', 'vencimiento4', 'cuota4', 'moneda', 'unid_medida', 'traslado', 'lugar_entrega', 'placa',
                 'conductor', 'datos_adicionales'])



    # Iterar sobre todas las hojas del libro
    for sheet_name in workbook.sheetnames:
        # Seleccionar la hoja actual
        sheet = workbook[sheet_name]

        # Recuperar la primera fila
        pedido = sheet['A1'].value

        # Crear una lista de listas con el resto del contenido de la hoja
        resto_filas = [[cell.value for cell in row[:25]] for row in sheet.iter_rows(min_row=2)]

        # Convertir el resto del contenido en un DataFrame
        df = pd.DataFrame(resto_filas[1:], columns=resto_filas[0])

        # Agregar cod_pedido de la celda A1
        df['cod_pedido'] = pedido

        # Eliminar columnas total y peso_total
        df.drop(['total', 'peso_total'], axis=1, inplace=True)

        # Unir verticalmente al dataframe cotizaciones solo si ningun valor de la columna cuo es nulo
        if not df['cuo'].isnull().any():
            cotizaciones = pd.concat([cotizaciones, df], ignore_index=True, axis=0)

    # Rellenar vacios en traslado con fecha de emision
    cotizaciones['traslado'] = cotizaciones['traslado'].infer_objects(copy=False).fillna(cotizaciones['emision'])

    # Cambiar datatype de las columnas de fecha a datetime con formato %Y-%m-%d
    cotizaciones[['emision', 'vencimiento', 'vencimiento2', 'vencimiento3', 'vencimiento4', 'traslado']] = cotizaciones[
        ['emision', 'vencimiento', 'vencimiento2', 'vencimiento3', 'vencimiento4', 'traslado']].apply(pd.to_datetime,
                                                                                                      format='%Y-%m-%d')

    # Cambiar nombre de columna lugar_entrega a llegada
    cotizaciones.rename(columns={'lugar_entrega': 'llegada'}, inplace=True)

    # Concatenar cod_pedido y cuo en columna cui
    cotizaciones['cui'] = cotizaciones['cod_pedido'] + cotizaciones['cuo'].astype(str)

    # Definir columnas de texto
    str_columns = ['alias', 'moneda', 'descripcion', 'unid_medida', 'llegada', 'datos_adicionales',
                   'observaciones']

    # Definir columnas a formatear
    columns_to_round = ['precio_unit', 'cuota1', 'cuota2', 'cuota3', 'cuota4']

    # Aplica redondeo a cada columna en la lista
    for col in columns_to_round:
        cotizaciones[col] = cotizaciones[col].apply(lambda x: round(x, 3) if pd.notna(x) else x)

    # Cambiar mayusculas y quitar espacios de los extremos a cada valor de cada columna de texto
    for column in str_columns:
        if cotizaciones[column].notna().any():
            cotizaciones[column] = cotizaciones[column].apply(lambda x: x.strip().upper() if pd.notna(x) else x)

    # Separar columnas de facturacion
    facturas = cotizaciones[['cod_pedido', 'cuo', 'alias', 'emision', 'moneda', 'descripcion', 'unid_medida',
                             'cantidad', 'precio_unit', 'observaciones', 'vencimiento', 'cuota1',
                             'vencimiento2', 'cuota2', 'vencimiento3', 'cuota3', 'vencimiento4', 'cuota4']]

    # Definir funcion de agrupamiento para las guias
    def columnas_guia(group):
        guia = group.iloc[0].copy()  # Tomar el primer valor de cada columna
        total_peso = (group['cantidad'] * group['peso_articulo']).sum()  # Sumar el producto de cantidad y peso
        guia['datos_adicionales'] = 'PesoTotal: ' + str(
            math.ceil(total_peso))  # Actualizar el valor de 'peso' en el primer registro

        return guia

    # Separar columnas para la tabla remision_remitente, aplicar funcion de agrupamiento y retirar columnas sobrantes
    remision_remitente = (cotizaciones[
                              ['cui', 'cod_pedido', 'cuo', 'alias', 'traslado', 'llegada', 'cantidad',
                               'peso_articulo', 'placa', 'conductor',
                               'datos_adicionales', 'observaciones']].groupby('cui').apply(columnas_guia,
                                                                                           include_groups=False)
                          .drop(['cantidad', 'peso_articulo'], axis=1))

    return print(facturas.to_sql('facturas', engine, if_exists='append', index=False),
                 remision_remitente.to_sql('remision_remitente', engine, if_exists='append', index=False))  ##


load_cotizaciones('D:/pytest/pedidos_20240701.xlsx')
