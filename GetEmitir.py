from datetime import datetime, timedelta
from time import sleep

import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def tofacturas(proveedores: str, dias: int):
    proveedores = [x.strip().upper() for x in proveedores.split(',')]

    if dias == 0 or dias is None:
        # Devuelve la fecha de hoy en formato SQL (yyyy-mm-dd)
        fechas = '= ' + datetime.today().strftime('%Y-%m-%d')
    else:
        # Calcula la diferencia de fechas y devuelve una cadena para usar en una consulta SQL
        fecha_inicio = datetime.today() - timedelta(days=dias)
        fecha_fin = datetime.today()
        fechas = f"BETWEEN '{fecha_inicio.strftime('%Y-%m-%d')}' AND '{fecha_fin.strftime('%Y-%m-%d')}'"

    salessystem = create_engine(
        'mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
        ':3306/salessystem')

    lista_facturas = pd.read_sql("SELECT * FROM lista_facturas WHERE emision " + fechas, con=salessystem,
                                 parse_dates=['emision', 'vencimiento', 'vencimiento2', 'vencimiento3', 'vencimiento4'])

    lista_guias = pd.read_sql("SELECT * FROM lista_guias", salessystem,
                              parse_dates=['traslado'])

    lista_proveedores = pd.read_sql('SELECT alias, numero_documento, usuario_sol, clave_sol FROM proveedores',
                                    salessystem)

    def formato_fecha(fecha):
        return pd.to_datetime(fecha, format='%Y-%m-%d').strftime('%d/%m/%Y')

    def formato_float(num):
        if pd.notna(num):
            # Convertir el número a string para contar los decimales
            num_str = f'{num:.10f}'  # Tomamos hasta 10 decimales para asegurarnos de capturar todos
            num_decimales = len(num_str.split('.')[1].rstrip('0'))  # Contar los decimales significativos

            # Mostrar hasta 4 decimales como máximo
            if num_decimales > 4:
                return f'{num:.4f}'
            elif 0 < num_decimales <= 2:
                return f'{num:.2f}'
            elif 2 < num_decimales <= 4:
                return f'{num:.{num_decimales}f}'
            else:
                return f'{num:.0f}'
        else:
            return ''

    # Aplicar las funciones de formato a las columnas respectivas
    lista_facturas['emision'] = lista_facturas['emision'].apply(formato_fecha)
    lista_facturas['vencimiento'] = lista_facturas['vencimiento'].apply(formato_fecha)
    lista_guias['traslado'] = lista_guias['traslado'].apply(formato_fecha)

    lista_facturas[['cantidad', 'p_unit']] = lista_facturas[['cantidad', 'p_unit']].applymap(formato_float)

    with pd.ExcelWriter('PorEmitir.xlsx', engine='xlsxwriter') as writer:
        workbook = writer.book
        formato1 = workbook.add_format({'bold': True, 'font_size': 12})
        formato2 = workbook.add_format({'italic': True, 'font_size': 10})
        formato3 = workbook.add_format({'font_size': 10})
        formato4 = workbook.add_format({'bold': True, 'font_size': 8})
        formato5_totales = workbook.add_format({'bg_color': '#FFFF00', 'font_size': 10})
        alineamiento = workbook.add_format({'align': 'right'})

        for proveedor in proveedores:
            lista_proveedor = lista_facturas[lista_facturas['alias'] == proveedor]
            # LISTA DE CUI SIN DUPLICADOS BASADA EN LAS FACTURAS
            lista_cui = lista_proveedor['cui'].drop_duplicates().tolist()

            if lista_proveedor.empty or len(lista_proveedor) == 0:
                break
            current_worksheet = workbook.add_worksheet(proveedor)
            fila = 0
            current_worksheet.write_row(fila, 0, lista_proveedores.loc[
                lista_proveedores['alias'] == proveedor].values.flatten().tolist(), formato4)
            fila += 1
            current_worksheet.write_row(fila, 0, ['CUI', 'GUIA', 'FACTURA', 'EMISION', 'ADQUIRIENTE', 'U. MED',
                                                  'DESCRIPCION', 'CANTIDAD', 'P. UNIT.', 'SUB-TOTAL', 'VENCIMIENTO',
                                                  'MONEDA'], formato1)
            fila += 1
            for cui in lista_cui:
                # SELECCIONAR LAS FACTURAS QUE COINCIDAN CON EL CUI SELECCIONADO RESETEANDO EL INDICE PARA QUE EMPIECE DESDE 0
                factura = lista_facturas[lista_facturas['cui'] == cui].reset_index(drop=False)
                # POR CADA INDICE Y FILA
                for index, row in factura.iterrows():
                    # SI EL INDICE ES 0 O ES LA PRIMERA LINEA DE LA FACTURA
                    if index == 0:
                        # COLOCAR TODOS LOS DATOS
                        current_worksheet.write_row(fila, 0,
                                                    (cui, row['guia'], row['numero'], row['emision'], row['ruc'],
                                                     row['unidad_medida'], row['descripcion'], row['cantidad'],
                                                     row['p_unit'], row['sub_total'], row['vencimiento'],
                                                     row['moneda']), formato3)
                        fila += 1
                        if len(factura) == 1:
                            current_worksheet.write_row(fila, 9, (factura['sub_total'].sum(),
                                                                  factura['sub_total'].sum() * 0.18,
                                                                  factura['sub_total'].sum() * 1.18), formato5_totales)
                            fila += 1
                            # CONSIDERAR COLOCAR CADA DETALLE DE LA GUIA CON SU ENCABEZADO EN FORMATO MAS PEQUENO
                            current_worksheet.write_row(fila, 1,
                                                        lista_guias[lista_guias['cui'] == cui].drop(['cui', 'alias'],
                                                                                                    axis=1).values.flatten().tolist(),
                                                        formato2)
                            # +2 REEMPLAZA A LA FILA VACIA QUE SE NECESITA
                            fila += 2
                    # SI EL INDICE ES EL ULTIMO COLOCAR TOTALES (en la emision solo figuran cantidad, p.unit, igv total y total por item)
                    elif index == len(factura) - 1:
                        current_worksheet.write_row(fila, 5, (row['unidad_medida'], row['descripcion'], row['cantidad'],
                                                              row['p_unit'], row['sub_total']), formato3)
                        fila += 1
                        # SUBTOTALIZAR CADA ARTICULO Y EL TOTAL DE LA FACTURA TAMBIEN CON SU ENCABEZADO PEQUENO
                        current_worksheet.write_row(fila, 9, (factura['sub_total'].sum(),
                                                              factura['sub_total'].sum() * 0.18,
                                                              factura['sub_total'].sum() * 1.18), formato5_totales)
                        fila += 1
                        # CONSIDERAR COLOCAR CADA DETALLE DE LA GUIA CON SU ENCABEZADO EN FORMATO MAS PEQUENO
                        current_worksheet.write_row(fila, 1,
                                                    lista_guias[lista_guias['cui'] == cui].drop(['cui', 'alias'],
                                                                                                axis=1).values.flatten().tolist(),
                                                    formato2)
                        # +2 REEMPLAZA A LA FILA VACIA QUE SE NECESITA
                        fila += 2
                    else:
                        current_worksheet.write_row(fila, 5, (row['unidad_medida'], row['descripcion'], row['cantidad'],
                                                              row['p_unit'], row['sub_total']), formato3)
                        fila += 1
            current_worksheet.set_column(0, 0, 12)  # COLUMNA CUI
            current_worksheet.set_column(1, 1, 10)  # COLUMNA GUIA
            current_worksheet.set_column(2, 2, 10)  # COLUMNA FACTURA
            current_worksheet.set_column(3, 3, 10)  # COLUMNA EMISION
            current_worksheet.set_column(4, 4, 11)  # COLUMNA ADQUIRIENTE
            current_worksheet.set_column(5, 5, 4)  # COLUMNA UNIDAD DE MEDIDA
            current_worksheet.set_column(6, 6, 45)  # COLUMNA DESCRIPCION
            current_worksheet.set_column(7, 8, None, alineamiento)
            current_worksheet.set_column(10, 10, 10)  # COLUMNA VENCIMIENTO


# TODO ORDENAR DE PROVEEDORES CON MENOS FACTURAS A PROVEEDORES CON MAS FACTURAS, ORGANIZAR MEJOR LOS DATOS DE LA GUIA PARA QUE SE MUESTREN MAS

tofacturas(
    'INVSONIC,SONICSERV,ENFOCATE,APOYO,INGCACH,INGCELIZ,ESPINO',
    2)#
