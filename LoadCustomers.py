import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def load_customers(ruta: str):
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

    customers = pd.read_excel(ruta + '/importar.xlsx', sheet_name='customers', date_format='%d/%m/%Y',
                                   dtype={'ruc': np.int64}, na_values=' ')

    str_columns = ['alias', 'nombre_razon', 'related_user', 'observaciones']

    # sumar peso total de los articulos por cotizacion y colocarlo en datos adicionales de guia
    for column in str_columns:
        if customers[column].notna().any():
            customers[column] = customers[column].apply(lambda x: x.strip().upper() if pd.notna(x) else x)

    return print(customers.to_sql('customers', engine, if_exists='append', index=False))  #


load_customers('C:/Users/Raknaros/Desktop/temporal')
