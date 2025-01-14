from datetime import datetime, timedelta

import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def tofacturas(proveedores: list = None, fecha: datetime = None, pedidos: list = None):
    salessystem = create_engine(
        'mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
        ':3306/salessystem')
    lista_facturas = pd.read_sql("SELECT * FROM lista_facturas", con=salessystem,
                                 parse_dates=['emision', 'vencimiento', 'vencimiento2', 'vencimiento3', 'vencimiento4'])
    if pedidos is None:
        fecha_inicio = datetime.now() - timedelta(days=3)
        fecha_inicio = fecha_inicio.strftime('%Y-%m-%d')
        fecha_fin = fecha.strftime('%Y-%m-%d')
        lista_facturas = lista_facturas.loc[(lista_facturas['emision'] >= fecha_inicio) &
                                            (lista_facturas['emision'] <= fecha_fin) &
                                            (lista_facturas['alias'].isin(proveedores))]
    else:
        lista_facturas = lista_facturas[lista_facturas['cui'].str.slice(0, 9).isin(pedidos)]

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
        #TODO VERIFICAR SI AGREGAR 3 DECIMALES SOLO AL PRECIO Y QUE TODOS LOS VALORES SALGAN AL MENOS CON DOS DECIMALES, COMPLETADOS CON CEROS

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

        for proveedor in lista_facturas['alias'].unique().tolist():
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
            current_worksheet.write_row(fila, 0,
                                        ['CUI', 'GUIA', 'FACTURA', '', '', '', '', '', 'ADQUIRIENTE', 'EMISION',
                                         'CANTIDAD',
                                         'U. MED', 'DESCRIPCION', 'P. UNIT.', 'SUB-TOTAL', 'VENCIMIENTO'],
                                        formato1)
            fila += 1
            for cui in lista_cui:
                # SELECCIONAR LAS FACTURAS QUE COINCIDAN CON EL CUI SELECCIONADO RESETEANDO EL INDICE PARA QUE EMPIECE DESDE 0
                factura = lista_facturas[lista_facturas['cui'] == cui].reset_index(drop=False)
                # POR CADA INDICE Y FILA
                for index, row in factura.iterrows():
                    #CARACTERISTICAS INICIALES`
                    caracteristicas = []
                    for caracteristica in [row['forma_pago'], row['moneda'], row['detraccion']]:
                        if caracteristica is not None:
                            #print(caracteristica)
                            if caracteristica == 'CREDITO':
                                caracteristicas.append('CRED')
                            elif caracteristica == 'USD':
                                caracteristicas.append('USD')
                            elif isinstance(caracteristica, int):
                                caracteristicas.append('SPOT')
                    # SI EL INDICE ES 0 O ES LA PRIMERA LINEA DE LA FACTURA
                    if index == 0:
                        # COLOCAR TODOS LOS DATOS
                        #PARTE 1
                        current_worksheet.write_row(fila, 0,
                                                    (cui, row['guia'], row['numero'], '', '',
                                                     '', '', '', row['ruc'],
                                                     row['emision'], row['cantidad'], row['unidad_medida'],
                                                     row['descripcion'], row['p_unit'], row['sub_total']
                                                     ), formato3)
                        #PARTE 2
                        current_worksheet.write_row(fila, 4, caracteristicas, formato3)
                        #PARTE 3
                        if row['vencimiento'] is not None:
                            current_worksheet.write_row(fila, 15,
                                                        [row['vencimiento']], formato3)
                        fila += 1
                        if len(factura) == 1:
                            current_worksheet.write_row(fila, 14, (factura['sub_total'].sum(),
                                                                   factura['sub_total'].sum() * 0.18,
                                                                   factura['sub_total'].sum() * 1.18), formato5_totales)
                            fila += 1
                            # CONSIDERAR COLOCAR CADA DETALLE DE LA GUIA CON SU ENCABEZADO EN FORMATO MAS PEQUENO
                            current_worksheet.write_row(fila, 8,
                                                        lista_guias[lista_guias['cui'] == cui].drop(['cui', 'alias'],
                                                                                                    axis=1).values.flatten().tolist(),
                                                        formato2)
                            #TODO COLOCAR INFORMACION DE LA DETRACCION EN LA MISMA FILA DE LA GUIA EN LA COLUMNA 18
                            # +2 REEMPLAZA A LA FILA VACIA QUE SE NECESITA
                            #TODO AGREGAR A LA FILA DIVISORIA ALGUN COLOR O LINEA DE MARGEN
                            fila += 3
                    # SI EL INDICE ES EL ULTIMO COLOCAR TOTALES (en la emision solo figuran cantidad, p.unit, igv total y total por item)
                    elif index == len(factura) - 1:

                        current_worksheet.write_row(fila, 10,
                                                    (row['cantidad'], row['unidad_medida'], row['descripcion'],
                                                     row['p_unit'], row['sub_total']), formato3)
                        fila += 1
                        # SUBTOTALIZAR CADA ARTICULO Y EL TOTAL DE LA FACTURA TAMBIEN CON SU ENCABEZADO PEQUENO
                        current_worksheet.write_row(fila, 14, (factura['sub_total'].sum(),
                                                               factura['sub_total'].sum() * 0.18,
                                                               factura['sub_total'].sum() * 1.18), formato5_totales)
                        fila += 1
                        # CONSIDERAR COLOCAR CADA DETALLE DE LA GUIA CON SU ENCABEZADO EN FORMATO MAS PEQUENO
                        #TODO AGREGAR INFORMACION DE DETRACCION EN LA FILA DE LA GUIA AL FINAL Y REORDENAR ORDEN DE LA INFO DE LA GUIA (PESO, PARTIDA, LLEGADA, PLACA, LICENCIA, FECHA, OBSERVACION
                        current_worksheet.write_row(fila, 8,
                                                    lista_guias[lista_guias['cui'] == cui].drop(['cui', 'alias'],
                                                                                                axis=1).values.flatten().tolist(),
                                                    formato2)
                        # TODO COLOCAR INFORMACION DE LA DETRACCION EN LA MISMA FILA DE LA GUIA EN LA COLUMNA 18
                        # +2 REEMPLAZA A LA FILA VACIA QUE SE NECESITA
                        # TODO AGREGAR A LA FILA DIVISORIA ALGUN COLOR O LINEA DE MARGEN
                        fila += 3
                    else:

                        current_worksheet.write_row(fila, 10,
                                                    (row['cantidad'], row['unidad_medida'], row['descripcion'],
                                                     row['p_unit'], row['sub_total']), formato3)
                        fila += 1
            #MODIFICAR ANCHO DE LAS COLUMNAS DE OPCIONES INICIALES
            current_worksheet.set_column(0, 0, 12)  # COLUMNA CUI
            current_worksheet.set_column(1, 1, 7)  # COLUMNA GUIA
            current_worksheet.set_column(2, 2, 7)  # COLUMNA FACTURA
            current_worksheet.set_column(3, 7, 4)  # COLUMNAS CARACTERISTICAS
            current_worksheet.set_column(8, 8, 12)  # COLUMNA ADQUIRIENTE
            current_worksheet.set_column(9, 9, 10)  # COLUMNA EMISION
            current_worksheet.set_column(11, 11, 4)  # COLUMNA UNIDAD DE MEDIDA
            current_worksheet.set_column(12, 12, 45)  # COLUMNA DESCRIPCION
            current_worksheet.set_column(13, 14, None, alineamiento)
            current_worksheet.set_column(15, 15, 10)  # COLUMNA VENCIMIENTO


# TODO ORDENAR DE PROVEEDORES CON MENOS FACTURAS A PROVEEDORES CON MAS FACTURAS, ORGANIZAR MEJOR LOS DATOS DE LA GUIA PARA QUE SE MUESTREN MAS
# TODO PENDIENTE IMPLEMENTAR PAGO EN MAS DE 1 CUOTA
tofacturas(
    proveedores=['GREJULCA', 'PARJU'],
    fecha=datetime(2025, 1, 5))  #