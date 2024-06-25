import pandas as pd
import os
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

    cantidad_pedidos = '9'  #str(pedidos['adquiriente'].count())

    #observacion, si en algun campo de numero va algun espacio verificar como se parsea por el read_excel y corregir para este caso y otros
    #pedidos.to_sql('pedidos', engine, if_exists='append', index=False)
    sleep(2)

    pedidos = pd.read_sql(
        'SELECT cod_pedido, periodo, promedio_factura, importe_total, (SELECT alias FROM customers WHERE ruc = adquiriente) as alias, '
        'contado_credito FROM pedidos ORDER BY id DESC LIMIT ' + cantidad_pedidos,
        engine,
        parse_dates=['fecha'])
    encabezado = ['cuo', 'alias', 'emision', 'descripcion', 'cantidad', 'precio_unit', 'total',
                  'peso_articulo', 'peso_total', 'observaciones', 'vencimiento', 'cuota1', 'vencimiento2',
                  'cuota2', 'vencimiento3', 'cuota3', 'vencimiento4', 'cuota4', 'moneda',
                  'unid_medida', 'traslado', 'lugar_entrega', 'placa', 'conductor', 'datos_adicionales']

    with pd.ExcelWriter('cuadros.xlsx', engine='xlsxwriter') as writer:
        for cod_pedido in pedidos['cod_pedido'].tolist():
            workbook = writer.book
            current_worksheet = workbook.add_worksheet(cod_pedido)
            current_worksheet.write_row(0, 0,
                                        pedidos.loc[pedidos['cod_pedido'] == cod_pedido].values.flatten().tolist())
            current_worksheet.write_row(1, 0, encabezado)
            cell_format1 = workbook.add_format({'bold': True, 'font_size': 12})
            cell_format2 = workbook.add_format({'bold': False, 'font_size': 10})
            current_worksheet.set_row(0, None, cell_format1)
            current_worksheet.set_row(1, None, cell_format1)
            current_worksheet.set_column(0, 0, 7, cell_format2)
            current_worksheet.set_column(1, 1, 9, cell_format2)
            current_worksheet.set_column(2, 2, 11, cell_format2)
            current_worksheet.set_column(3, 3, 45, cell_format2)
            current_worksheet.write_formula('G3', 'ROUND(E3*F3*1.18;3)')
            current_worksheet.write_formula('I3', 'ROUNDUP(E3*H3;0)')

    return print('okey')  #


load_pedidos('D:/OneDrive/facturacion')
