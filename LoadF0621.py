import pandas as pd
import os
from sqlalchemy import create_engine
import psycopg2
import zipfile

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

archivo_zip = 'tus_archivos.zip'

nombre_archivo_csv = 'archivo.csv'

with zipfile.ZipFile(archivo_zip, 'r') as zipf:
    with zipf.open(nombre_archivo_csv) as archivo_csv:
        # Cargar el archivo CSV en un DataFrame de pandas
        df = pd.read_csv(archivo_csv)