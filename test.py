import pandas as pd
from datetime import date

from sqlalchemy import create_engine
import numpy as np

from services.Querys import salessystem, pedidos, adquirientes, pre_detalle, catalogo
from streamlit_authenticator.utilities.hasher import Hasher

passwords_to_hash = ['emisor2024', 'EvelynCBM1968', 'p259E9C695+']
#hashed_passwords = Hasher(passwords_to_hash).generate()

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def get_precuadros(ped_seleccionados):
    if ped_seleccionados == 'Todos':
        seleccionados = pedidos.loc[pedidos['estado'] == 'PENDIENTE']['cod_pedido'].tolist()
    else:
        seleccionados = [x.strip().upper() for x in ped_seleccionados.split(',')]

    pedidos_extendido = pd.merge(pedidos, adquirientes[['ruc', 'alias']], left_on='adquiriente', right_on='ruc',
                                 how='left')

    encabezado = ['cuo', 'alias', 'emision', 'descripcion', 'cantidad', 'precio_unit', 'total',
                  'peso_articulo', 'peso_total', 'observaciones', 'vencimiento', 'cuota1', 'vencimiento2',
                  'cuota2', 'vencimiento3', 'cuota3', 'vencimiento4', 'cuota4', 'moneda',
                  'unid_medida', 'traslado', 'lugar_entrega', 'placa', 'conductor', 'datos_adicionales']

    detalle_completo = pd.merge(pre_detalle, catalogo, on='descripcion', how='left')

    with pd.ExcelWriter('pedidos_' + date.today().strftime('%Y%m%d') + '.xlsx', engine='xlsxwriter') as writer:
        for cod_pedido in seleccionados:
            detalle = detalle_completo.loc[detalle_completo['numero_documento'] == str(int(pedidos_extendido[['ruc','cod_pedido']].loc[pedidos_extendido['cod_pedido'] == 'cod_pedido']['ruc'].values.item()))].sort_values(by='fecha_emision', ascending=False).head(30)
            try:
                workbook = writer.book
                current_worksheet = workbook.add_worksheet(cod_pedido)
                #['cod_pedido','periodo','importe_total', 'rubro', 'promedio_factura', 'contado_credito''notas' 'punto_entrega''alias']
                current_worksheet.write_row(0, 0,
                                            pedidos_extendido[
                                                ['cod_pedido', 'periodo', 'alias', 'contado_credito', 'importe_total',
                                                 'promedio_factura', 'notas', 'rubro',
                                                 'punto_entrega', 'ruc']].loc[pedidos_extendido['cod_pedido'] == cod_pedido].values.flatten().tolist())
                current_worksheet.write_column(3, 2, detalle['descripcion'].tolist()) #descripcion
                current_worksheet.write_column(4, 2, detalle['cantidad'].tolist()) #cantidad
                current_worksheet.write_column(5, 2, detalle['precio'].tolist()) #precio
                current_worksheet.write_column(7, 2, detalle['peso'].tolist()) #peso
                current_worksheet.write_column(19, 2, detalle['unidad_medida_y'].tolist()) #unidad_medida

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


get_precuadros('Todos')