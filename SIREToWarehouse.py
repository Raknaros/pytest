import os
import zipfile
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

def etl_sire_compras(rutas_archivos: List[str], db_url: str, schema_db: str, tabla_db: str) -> pd.DataFrame | None:
    """
    Proceso ETL completo para leer MÚLTIPLES archivos (CSV o TXT), transformar los datos
    y cargarlos a una base de datos PostgreSQL.
    """
    try:
        # --- 1. EXTRAER (VERSIÓN MEJORADA) ---
        lista_de_dataframes = []
        print("--- Iniciando fase de extracción ---")

        for ruta_archivo in rutas_archivos:
            try:
                # Verificar que el archivo existe antes de intentar leerlo
                if not os.path.exists(ruta_archivo):
                    print(f"❌ Error: No se encontró el archivo en la ruta: {ruta_archivo}")
                    continue  # Salta al siguiente archivo de la lista

                print(f"Procesando archivo: {ruta_archivo}")

                # Decidir el separador basado en la extensión del archivo
                if ruta_archivo.lower().endswith('.csv'):
                    df_temp = pd.read_csv(ruta_archivo, sep=',', header=0, dtype=str)
                    lista_de_dataframes.append(df_temp)
                elif ruta_archivo.lower().endswith('.txt'):
                    df_temp = pd.read_csv(ruta_archivo, sep='|', header=0, dtype=str)
                    lista_de_dataframes.append(df_temp)
                else:
                    # Ignorar archivos con extensiones no soportadas
                    print(
                        f"⚠️ Advertencia: El archivo '{os.path.basename(ruta_archivo)}' tiene un formato no soportado y será ignorado.")

            except Exception as e:
                print(f"❌ Error al leer el archivo '{os.path.basename(ruta_archivo)}': {e}")

        # Si no se pudo leer ningún archivo, detener el proceso
        if not lista_de_dataframes:
            print("No se cargaron datos válidos. Finalizando proceso ETL.")
            return None

    except:
        None
# --- Ejemplo de Uso ---
if __name__ == "__main__":
    load_dotenv()

    # Parámetros del proceso
    ruta_del_zip = ["C:/Users/Raknaros/Downloads/SIRE_setiembre/SIRE/SEPTIEMBRE/20614551373-20251008-200407-propuesta.zip", ]
    db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

"""
    # Llamar a la función principal del ETL
    dataframe_cargado = etl_sire_compras(
        ruta_zip=ruta_del_zip,
        db_url=db_url,
        schema_db="acc",
        tabla_db="_5"
    )
    # Si el ETL fue exitoso, generar el resumen
    if dataframe_cargado is not None:
        generar_resumen_exploratorio(dataframe_cargado)
"""