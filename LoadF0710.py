import pandas as pd
import os
from sqlalchemy import create_engine
import zipfile
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


# .assign(ruc=i[0:11])
def load_f0710(carpeta: str):
    set_f0710 = pd.DataFrame()
    try:
        # Construir la URL de conexión desde variables de entorno
        db_url = f"{os.getenv('DB_WAREHOUSE_DIALECT')}://{os.getenv('DB_WAREHOUSE_USER')}:{os.getenv('DB_WAREHOUSE_PASSWORD')}@{os.getenv('DB_WAREHOUSE_HOST')}:{os.getenv('DB_WAREHOUSE_PORT')}/{os.getenv('DB_WAREHOUSEE_NAME')}"
        engine = create_engine(db_url)
        
        # Validar la conexión
        with engine.connect() as conn:
            pass
            
    except Exception as e:
        print(f"Error al conectar a la base de datos: {str(e)}")
        return None
    for i in list(filter(lambda x: (x[12:16] == '0710'), os.listdir(carpeta))):
        with (zipfile.ZipFile(carpeta + '/' + i, 'r') as zipf):
            with zipf.open(i[28:36] + i[0:11] + 'pdt621_casillas.csv') as archivo_csv:
                f0621 = pd.read_csv(archivo_csv, usecols=[6, 7], header=0).T.reset_index(drop=True)

    return print(set_f0710)  # .to_sql('_9', engine, if_exists='append', index=False, schema='acc')


load_f0710('C:/Users/Raknaros/Desktop/temporal/declaraciones')
