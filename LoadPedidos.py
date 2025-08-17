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


def load_pedidos(ruta: str):
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

    #observacion, si en algun campo de numero va algun espacio verificar como se parsea por el read_excel y corregir para este caso y otros
    #

    return print(pedidos.to_sql('pedidos', engine, if_exists='append', index=False))  #


load_pedidos('D:/OneDrive/facturacion')
