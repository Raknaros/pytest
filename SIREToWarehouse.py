import os
import zipfile
import pandas as pd
import psycopg2
import io
from dotenv import load_dotenv


def procesar_zip_a_db(ruta_zip, db_params, tabla_db):
    """
    Procesa un archivo ZIP, carga su contenido (CSV o TXT) en un DataFrame,
    lo inserta en una base de datos PostgreSQL y muestra un resumen.

    Args:
        ruta_zip (str): La ruta completa al archivo .zip.
        db_params (dict): Un diccionario con los parámetros de conexión a la base de datos.
        tabla_db (str): El nombre de la tabla en la base de datos donde se insertarán los datos.
    """
    try:
        # 1. Abrir el archivo ZIP y revisar su contenido
        with zipfile.ZipFile(ruta_zip, 'r') as z:
            archivos_en_zip = z.namelist()
            if len(archivos_en_zip) != 1:
                print(
                    f"Error: El ZIP debe contener exactamente un archivo, pero se encontraron {len(archivos_en_zip)}.")
                return

            nombre_archivo = archivos_en_zip[0]
            print(f"Archivo encontrado en el ZIP: {nombre_archivo}")

            # 2. Extraer y cargar el archivo en un DataFrame de Pandas
            with z.open(nombre_archivo) as archivo:
                # La primera fila se usa como encabezado y luego se elimina del DataFrame final
                if nombre_archivo.lower().endswith('.csv'):
                    df = pd.read_csv(archivo, header=0)
                elif nombre_archivo.lower().endswith('.txt'):
                    df = pd.read_csv(archivo, sep='|', header=0)
                else:
                    print("Error: El archivo dentro del ZIP no es .csv ni .txt.")
                    return

        # Eliminar espacios en blanco de los nombres de las columnas
        df.columns = df.columns.str.strip()

        print(f"Carga exitosa en DataFrame.")

        # 4. Generar el resumen de los datos ingresados
        if not df.empty:
            total_registros = len(df)
            valor_columna_2 = df.iloc[0, 1]  # Valor de la primera celda de la segunda columna
            suma_columna_15 = df.iloc[:, 14].sum()  # Suma de la columna en la posición 14 (la 15ª)
            valor_columna_3 = df.iloc[0, 2]  # Valor de la primera celda de la tercera columna

            # --- NUEVOS CÁLCULOS SOLICITADOS ---

            # 1. Dividir la columna 16 sobre la 15 y redondear a 2 decimales
            # Se crea una Serie de Pandas con los resultados (0.10 o 0.18)
            proporcion = (df.iloc[:, 15] / df.iloc[:, 14]).round(2)

            # 2. Contar cuántas filas tienen 0.10 y cuántas 0.18
            conteo_proporcion = proporcion.value_counts()
            conteo_010 = conteo_proporcion.get(0.10, 0)
            conteo_018 = conteo_proporcion.get(0.18, 0)

            # 3. Sumar la columna 15 según el valor de la división
            columna_15 = df.iloc[:, 14]
            suma_agrupada = columna_15.groupby(proporcion).sum()
            suma_010 = suma_agrupada.get(0.10, 0)
            suma_018 = suma_agrupada.get(0.18, 0)

            print("\n--- Resumen de la Carga ---")
            print(f"Total de registros procesados: {total_registros}")
            print(f"Entidad: {valor_columna_2}")
            print(f"Periodo tributario: {valor_columna_3}")
            print(f"Suma total de la base imponible: {suma_columna_15:,.2f}")
            print("---------------------------\n")

            # 4. Imprimir el nuevo resumen
            print("--- Resumen por Proporción (Col 16 / Col 15) ---")
            print("Resultados IGV 10%:")
            print(f"  - Cantidad de registros: {conteo_010}")
            print(f"  - Suma de la base imponible: {suma_010:,.2f}")  # Formato con comas y 2 decimales

            print("\nResultados IGV 18%:")
            print(f"  - Cantidad de registros: {conteo_018}")
            print(f"  - Suma de la base imponible: {suma_018:,.2f}")  # Formato con comas y 2 decimales
            print("-------------------------------------------------\n")



    except FileNotFoundError:
        print(f"Error: No se encontró el archivo ZIP en la ruta: {ruta_zip}")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")


# --- Ejemplo de Uso ---
if __name__ == '__main__':
    # Ruta al archivo ZIP que quieres procesar
    ruta_del_zip = 'C:/Users/Raknaros/Downloads/20606283858-20250718-225604-propuesta.zip'

    load_dotenv()
    # Parámetros de conexión a tu base de datos PostgreSQL
    parametros_db = {
        "dbname": os.getenv("DB_WAREHOUSE_NAME"),
        "user": os.getenv("DB_WAREHOUSE_USER"),
        "password": os.getenv("DB_WAREHOUSE_PASSWORD"),
        "host": os.getenv("DB_WAREHOUSE_HOST"),
        "port": os.getenv("DB_WAREHOUSE_PORT")
    }

    # Nombre de la tabla de destino en la base de datos
    tabla_destino = "sire"

    # Llamar a la función
    procesar_zip_a_db(ruta_del_zip, parametros_db, tabla_destino)

"""
        # 3. Insertar la información en la base de datos
        conn = None  # Inicializar la conexión como None
        try:
            conn = psycopg2.connect(**db_params)
            cursor = conn.cursor()
            print("Conexión a la base de datos exitosa.")

            # Preparar los datos para una inserción eficiente con execute_values
            # Se asume que el orden de las columnas en el DataFrame coincide con la tabla
            tuples = [tuple(x) for x in df.to_numpy()]
            cols = ','.join(list(df.columns))

            # Crear la consulta de inserción
            query = f"INSERT INTO {tabla_db} ({cols}) VALUES %s"

            # Usar execute_values para una inserción masiva
            from psycopg2.extras import execute_values
            execute_values(cursor, query, tuples)

            conn.commit()
            print(f"Se insertaron {len(df)} registros en la tabla '{tabla_db}'.")

        except Exception as e:
            print(f"Error en la base de datos: {e}")
            if conn:
                conn.rollback()  # Revertir cambios en caso de error
        finally:
            if conn:
                cursor.close()
                conn.close()
                print("Conexión a la base de datos cerrada.")
"""