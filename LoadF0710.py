import pandas as pd
import os
from sqlalchemy import create_engine
import psycopg2
import zipfile

# Ruta del archivo ZIP
archivo_zip = 'tus_archivos.zip'

# Nombre del archivo CSV dentro del ZIP
nombre_archivo_csv = 'archivo.csv'

# Crear un objeto ZipFile
with zipfile.ZipFile(archivo_zip, 'r') as zipf:
    # Leer el archivo CSV directamente desde el archivo ZIP
    with zipf.open(nombre_archivo_csv) as archivo_csv:
        # Cargar el archivo CSV en un DataFrame de pandas
        df = pd.read_csv(archivo_csv)