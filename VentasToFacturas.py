from time import sleep
import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def tofacturas(periodo: str):
    try:
        # Conexión a SalesSystem (Facturación)
        salessystem_url = f"{os.getenv('DB_SALESSYSTEM_DIALECT')}://{os.getenv('DB_SALESSYSTEM_USER')}:{os.getenv('DB_SALESSYSTEM_PASSWORD')}@{os.getenv('DB_SALESSYSTEM_HOST')}:{os.getenv('DB_SALESSYSTEM_PORT')}/{os.getenv('DB_SALESSYSTEM_NAME')}"
        salessystem = create_engine(salessystem_url)
        
        # Conexión a Warehouse (Contabilidad)
        warehouse_url = f"{os.getenv('DB_WAREHOUSE_DIALECT')}://{os.getenv('DB_WAREHOUSE_USER')}:{os.getenv('DB_WAREHOUSE_PASSWORD')}@{os.getenv('DB_WAREHOUSE_HOST')}:{os.getenv('DB_WAREHOUSE_PORT')}/{os.getenv('DB_WAREHOUSEE_NAME')}"
        warehouse = create_engine(warehouse_url)
        
        # Validar las conexiones
        with salessystem.connect() as conn_sales:
            pass
        with warehouse.connect() as conn_warehouse:
            pass
            
    except Exception as e:
        print(f"Error al conectar a las bases de datos: {str(e)}")
        return None

    #Consultar vista facturas_salessystem de warehouse segun periodo indicado
    ventas = pd.read_sql('SELECT * FROM facturas_salessystem WHERE periodo_tributario =' + periodo, warehouse,
                         parse_dates=['fecha'], dtype={'periodo_tributario': int, 'ruc': np.int64})

    #Luego obtener el alias desde el ruc para colocar el alias en proveedor y no el ruc
    #Funcion de transformacion de columnas agrupadas
    def columnas_pedidos(group):
        #Fecha minima menos 2 dias
        uno = group['emision'].min(skipna=True) - pd.Timedelta(days=2)
        #Fecha minima convertida a formato YYYYMM
        dos = group['emision'].min().strftime('%Y%m')
        #Identificar si alguna factura es a credito o si es al contado
        tres = 'CREDITO' if 'CREDITO' in group['forma_pago'].values else 'CONTADO'
        #Multiplicar cantidad por precio unitario y agregarle el IGV, redondeado a entero
        cuatro = np.floor((group['cantidad'] * group['precio_unit']).sum() * 1.18).astype(int)
        #Colocar ENTREGADO como estado
        cinco = 'ENTREGADO'
        return pd.Series({
            'fecha_pedido': uno,
            'periodo': dos,
            'contado_credito': tres,
            'importe_total': cuatro,
            'estado': cinco
        })

    #Crear dataframe agrupado de pedidos
    pedidos = ventas.groupby('ruc').apply(columnas_pedidos, include_groups=False).reset_index()
    #Cambiar nombre de la columna ruc a adquiriente
    pedidos.rename(
        columns={'ruc': 'adquiriente'},
        inplace=True)
    #Contar los pedidos
    cantidad_pedidos = str(pedidos['adquiriente'].count())
    #Insertar los pedidos en salessystem para generar el codigo de pedido
    #pedidos.to_sql('pedidos', salessystem, if_exists='append', index=False)
    sleep(2)
    #Consultar los pedidos segun la cantidad insertada para obtenerlos con el codigo de pedido
    pedidos = pd.read_sql('SELECT cod_pedido, adquiriente FROM pedidos ORDER BY id DESC LIMIT ' + cantidad_pedidos,
                          salessystem,
                          parse_dates=['fecha'])
    #Cambiar de nombre la columna de adquiriente a ruc
    pedidos.rename(columns={'adquiriente': 'ruc'}, inplace=True)
    #Hacer merge con el dataframe ventas para asignar el codigo de pedido segun el adquiriente
    facturas = ventas.merge(pedidos, on='ruc', how='inner')
    #Concatenar alias, serie y numero en la columna factura
    facturas['factura'] = facturas['alias'].astype(str) + '_' + facturas['serie'].astype(str) + '_' + facturas[
        'numero'].astype(str)
    # Reiniciar el conteo para cada cliente
    facturas['cuo'] = facturas.groupby('cod_pedido')['factura'].transform(lambda x: pd.factorize(x)[0] + 1)
    # Eliminar la columna 'factura' si no es necesaria en el resultado final
    facturas.drop(columns=['factura', 'periodo_tributario'], inplace=True)
    # Ordenar antes de insertar
    facturas.sort_values(['ruc', 'alias', 'numero'], ascending=False)
    #Antes de la ejecucion ordenar por cod_pedido, alias, factura
    return facturas.to_sql('facturas', salessystem, if_exists='append', index=False)  #


print(tofacturas('202405'))

#TODO PENDIENTE IMPLEMENTAR TRIPLE DECIMAL, RETENCION Y DETRACCION DESDE PARSER SUNAT HASTA ESTA FUNCION
