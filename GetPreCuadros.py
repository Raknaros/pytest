import pandas as pd
from datetime import date
from time import sleep
from sqlalchemy import create_engine
import numpy as np
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def get_precuadros():
    try:
        # Construir la URL de conexión desde variables de entorno
        db_url = f"{os.getenv('DB_SALESSYSTEM_DIALECT')}://{os.getenv('DB_SALESSYSTEM_USER')}:{os.getenv('DB_SALESSYSTEM_PASSWORD')}@{os.getenv('DB_SALESSYSTEM_HOST')}:{os.getenv('DB_SALESSYSTEM_PORT')}/{os.getenv('DB_SALESSYSTEM_NAME')}"
        engine = create_engine(db_url)
        
        # Validar la conexión
        with engine.connect() as conn:
            pass
            
    except Exception as e:
        print(f"Error al conectar a la base de datos: {str(e)}")
        return None

    pedidos = pd.read_sql(
        "SELECT cod_pedido, periodo, (SELECT alias FROM customers WHERE ruc = adquiriente) as alias, contado_credito, importe_total, promedio_factura"
        " FROM pedidos WHERE estado = 'PENDIENTE'",
        engine,
        parse_dates=['fecha'])
    encabezado = ['cuo', 'alias', 'emision', 'descripcion', 'cantidad', 'precio_unit', 'total',
                  'peso_articulo', 'peso_total', 'observaciones', 'vencimiento', 'cuota1', 'vencimiento2',
                  'cuota2', 'vencimiento3', 'cuota3', 'vencimiento4', 'cuota4', 'moneda',
                  'unid_medida', 'traslado', 'lugar_entrega', 'placa', 'conductor', 'datos_adicionales']

    with pd.ExcelWriter('pedidos_' + date.today().strftime('%Y%m%d') + '.xlsx', engine='xlsxwriter') as writer:
        for cod_pedido in pedidos['cod_pedido'].tolist():
            try:
                workbook = writer.book
                current_worksheet = workbook.add_worksheet(cod_pedido)
                current_worksheet.write_row(0, 0,
                                            pedidos.loc[pedidos['cod_pedido'] == cod_pedido].values.flatten().tolist())
                current_worksheet.write_column()
            except:
                None
            finally:
                current_worksheet.write_row(1, 0, encabezado)
                cell_format1 = workbook.add_format({'bold': True, 'font_size': 12})
                cell_format2 = workbook.add_format({'bold': False, 'font_size': 10})
                current_worksheet.set_row(0, None, cell_format1)
                current_worksheet.set_row(1, None, cell_format1)
                current_worksheet.set_column(0, 0, 7, cell_format2)
                current_worksheet.set_column(1, 1, 9, cell_format2)
                current_worksheet.set_column(2, 2, 11, cell_format2)
                current_worksheet.set_column(3, 3, 45, cell_format2)
                current_worksheet.write('G3', 'ROUND(E3*F3*1.18;3)')
                current_worksheet.write('I3', 'ROUNDUP(E3*H3;0)')



    return print('pedidos_' + date.today().strftime('%Y%m%d') + ' generado')


get_precuadros()
