from time import sleep

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
                         parse_dates=['fecha'], dtype={'periodo_tributario': int, 'ruc': np.int64, 'alias': object})

    # Es necesario filtrar solo los proveedores que esten dentro de la lista de proveedores de salessystem

    # Luego obtener el alias desde el ruc para colocar el alias en proveedor y no el ruc

    def columnas_pedidos(group):
        uno = group['emision'].min(skipna=True) - pd.Timedelta(days=2)
        dos = group['emision'].min().strftime('%Y%m')
        tres = 'CREDITO' if 'CREDITO' in group['forma_pago'].values else 'CONTADO'
        cuatro = np.floor((group['cantidad'] * group['precio_unit']).sum() * 1.18).astype(int)
        cinco = 'ENTREGADO'
        return pd.Series({
            'fecha_pedido': uno,
            'periodo': dos,
            'contado_credito': tres,
            'importe_total': cuatro,
            'estado': cinco
        })

    pedidos = ventas.groupby('ruc').apply(columnas_pedidos, include_groups=False).reset_index()
    pedidos.rename(
        columns={'ruc': 'adquiriente'},
        inplace=True)
    cantidad_pedidos = str(pedidos['adquiriente'].count())
    # pedidos.to_sql('pedidos', salessystem, if_exists='append', index=False)
    sleep(2)
    pedidos = pd.read_sql('SELECT cod_pedido, adquiriente FROM pedidos ORDER BY id DESC LIMIT ' + cantidad_pedidos,
                          salessystem,
                          parse_dates=['fecha'])
    pedidos.rename(columns={'adquiriente': 'ruc'}, inplace=True)
    facturas = ventas.merge(pedidos, on='ruc', how='inner')

    return print(facturas.dtypes)  # .to_sql('facturas', salessystem, if_exists='append', index=False)


tofacturas('202405')

