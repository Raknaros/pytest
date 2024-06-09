import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def tofacturas(periodo: str):
    salessystem = create_engine(
        'mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
        ':3306/salessystem')

    warehouse = create_engine('postgresql://admindb:72656770@datawarehouse.cgvmexzrrsgs.us-east-1.rds.amazonaws.com'
                              ':5432/warehouse')

    ventas = pd.read_sql('SELECT * FROM facturas_salessystem WHERE periodo_tributario =' + periodo, warehouse,
                         parse_dates=['fecha'])

    #Es necesario filtrar solo los proveedores que esten dentro de la lista de proveedores de salessystem

    #Luego obtener el alias desde el ruc para colocar el alias en proveedor y no el ruc
    def conteo_fecha():
        fechas_min = ventas.groupby('ruc').agg(fecha_min=('emision', 'min')).reset_index()
        # Inicializar un diccionario para llevar el conteo acumulativo de fechas
        date_count = {}
        # Crear una lista para almacenar el conteo personalizado
        conteo_fecha_repetida = []
        # Iterar sobre el DataFrame agrupado
        for date in fechas_min['fecha_min']:
            if date not in date_count:
                date_count[date] = 1
            else:
                date_count[date] += 1
            conteo_fecha_repetida.append(date_count[date])
        return conteo_fecha_repetida

    def credito_o_contado(tipos):
        return 'CREDITO' if 'CREDITO' in tipos.values else 'CONTADO'

    def min_date_to_int(x):
        return int(x.min().strftime('%Y%m'))

    def multiply_and_round_down(sub_df):
        return np.floor((sub_df['cantidad'] * sub_df['precio_unit']).sum() * 1.18).astype(int)

    result = ventas.groupby('ruc').agg(
        fecha_pedido=('emision', 'min'),
        periodo=('emision', min_date_to_int),
        suma_multiplicacion=('ruc', lambda x: multiply_and_round_down(ventas[ventas['ruc'] == x.name])),
        credito_contado=('forma_pago', credito_o_contado)
    ).reset_index()

    #funcion para concatenar resultado y construir el cod_pedido
    def concatenar_resultados(grupo_df):
        fecha_min = grupo_df['emision'].min()
        ultimo_dia_mes = pd.to_datetime(f'{fecha_min.year}-{fecha_min.month}-01') + pd.offsets.MonthEnd(1)
        dias_diferencia = (ultimo_dia_mes - fecha_min).days

        valor_fecha = 0 if dias_diferencia < 4 else 1
        fecha_min_hex = hex(int(fecha_min.strftime('%Y%m%d')))[2:]
        conteo_fecha_min = (grupo_df['fecha'] == fecha_min).sum()
        numero_orden = conteo_fecha_min

    # Añadir la lista de conteos personalizados al DataFrame agrupado
    result['conteo_fecha_repetida'] = conteo_fecha()

    # Convertir la columna 'conteo_fecha_repetida' a string
    result['conteo_fecha_repetida'] = result['conteo_fecha_repetida'].astype(str)

    return print(result)  #.to_sql('facturas', salessystem, if_exists='append', index=False)


tofacturas('202405')
