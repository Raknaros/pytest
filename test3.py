import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from typing import List


def leer_cuis_de_archivo(ruta_archivo: str) -> List[str]:
    """
    Lee un archivo de texto y devuelve una lista limpia de CUIs,
    eliminando espacios en blanco y líneas vacías.
    """
    try:
        with open(ruta_archivo, 'r') as f:
            cuis = [line.strip() for line in f if line.strip()]
        print(f"✅ Se leyeron {len(cuis)} CUIs del archivo '{ruta_archivo}'.")
        return cuis
    except FileNotFoundError:
        print(f"❌ Error: El archivo '{ruta_archivo}' no fue encontrado.")
        return []


def consultar_facturas_por_cui(cui_list: List[str]):
    """
    Se conecta a la base de datos Warehouse, ejecuta la consulta
    y devuelve los resultados como un DataFrame de Pandas.
    """
    if not cui_list:
        print("⚠️ La lista de CUIs está vacía. No se ejecutará la consulta.")
        return None

    load_dotenv()

    db_url = (
        f"{os.getenv('DB_WAREHOUSE_DIALECT')}://"
        f"{os.getenv('DB_WAREHOUSE_USER')}:{os.getenv('DB_WAREHOUSE_PASSWORD')}@"
        f"{os.getenv('DB_WAREHOUSE_HOST')}:{os.getenv('DB_WAREHOUSE_PORT')}/"
        f"{os.getenv('DB_WAREHOUSE_NAME')}"
    )

    try:
        engine = create_engine(db_url)

        # --- CAMBIO 1: Modificar la Consulta SQL ---
        # Añadimos la columna 'cui' al SELECT para poder cruzarla después.
        query = text(
            """
            SELECT cui, periodo_tributario, ruc, numero_documento 
            FROM acc._5 
            WHERE cui IN :cui_list
            """
        )

        with engine.connect() as connection:
            print("🚀 Ejecutando consulta en la base de datos...")
            result = connection.execute(query, {"cui_list": tuple(cui_list)})

            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            print(f"✅ Consulta exitosa. Se encontraron {len(df)} registros.")
            return df

    except Exception as e:
        print(f"❌ Error al conectar o consultar la base de datos: {e}")
        return None


# --- Ejecución Principal ---
if __name__ == "__main__":

    archivo_de_cuis = "C:/Users/Raknaros/Downloads/cui_bancarizados.txt"
    lista_de_cuis_buscados = leer_cuis_de_archivo(archivo_de_cuis)

    if lista_de_cuis_buscados:
        df_resultados_db = consultar_facturas_por_cui(lista_de_cuis_buscados)

        if df_resultados_db is not None:
            # --- CAMBIO 2: Lógica de Comparación con Pandas ---

            # 1. Convertimos la lista original de CUIs a un DataFrame.
            df_buscados = pd.DataFrame(lista_de_cuis_buscados, columns=['cui_buscado'])

            # 2. Hacemos un "left merge". Esto mantiene TODOS los CUIs que buscamos
            #    y une los resultados de la base de datos que coincidan.
            #    Si un CUI no se encontró, sus columnas (periodo, ruc, etc.) quedarán como NaN.
            df_final = pd.merge(df_buscados, df_resultados_db, left_on='cui_buscado', right_on='cui', how='left')

            # 3. Creamos una columna 'encontrado' para que sea más claro.
            #    Será 'Sí' si la columna 'ruc' (que viene de la BD) no es nula.
            df_final['encontrado'] = df_final['ruc'].notna().map({True: 'Sí', False: 'No'})

            # 4. Limpiamos el DataFrame para la presentación.
            df_final = df_final.drop(columns=['cui'])  # Eliminamos la columna 'cui' duplicada.
            df_final = df_final.fillna('')  # Reemplazamos los NaN con texto vacío.

            # 5. Reordenamos las columnas para un mejor reporte.
            df_final = df_final[['cui_buscado', 'encontrado', 'periodo_tributario', 'ruc', 'numero_documento']]

            # Muestra el reporte final
            #print("\n--- Reporte de Conciliación ---")
            #print(df_final.to_string())  # .to_string() para asegurar que se muestren todas las filas

            # Opcional: Guardar el nuevo reporte en un archivo Excel
            df_final.to_excel("reporte_facturas.xlsx", index=False)
            print("\nReporte guardado en 'reporte_facturas.xlsx'")