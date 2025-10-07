import os
import zipfile

import numpy as np
import pandas as pd
import psycopg2
import io
from dotenv import load_dotenv

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

ruta_zip = "C:/Users/Raknaros/Downloads/20614551373-20251006-174102-propuesta.zip"

try:
    # 1. Abrir el archivo ZIP y revisar su contenido
    with zipfile.ZipFile(ruta_zip, 'r') as z:
        archivos_en_zip = z.namelist()
        if len(archivos_en_zip) != 1:
            print(
                f"Error: El ZIP debe contener exactamente un archivo, pero se encontraron {len(archivos_en_zip)}.")

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
except FileNotFoundError:
    print(f"Error: No se encontró el archivo ZIP en la ruta: {ruta_zip}")
except Exception as e:
    print(f"Ocurrió un error inesperado: {e}")

# --- Seleccionar solo las columnas que te interesan ---
df = df[['RUC', 'Periodo', 'CAR SUNAT', 'Fecha de emisión', 'Fecha Vcto/Pago', 'Tipo CP/Doc.', 'Serie del CDP',
         'Nro CP o Doc. Nro Inicial (Rango)', 'Tipo Doc Identidad', 'Nro Doc Identidad', 'BI Gravado DG',
         'IGV / IPM DG', 'BI Gravado DGNG', 'IGV / IPM DGNG', 'BI Gravado DNG', 'IGV / IPM DNG',
         'Valor Adq. NG', 'ISC', 'ICBPER', 'Otros Trib/ Cargos', 'Moneda', 'Tipo CP Modificado', 'Serie CP Modificado',
         'Nro CP Modificado', 'Detracción']]

# --- Renombrar columnas para que coincidan con tu base de datos ---
df = df.rename(columns={
    'RUC': 'ruc',
    'Periodo': 'periodo_tributario',
    'CAR SUNAT': 'observaciones',
    'Fecha de emisión': 'fecha_emision',
    'Fecha Vcto/Pago': 'fecha_vencimiento',
    'Tipo CP/Doc.': 'tipo_comprobante',
    'Serie del CDP': 'numero_serie',
    'Nro CP o Doc. Nro Inicial (Rango)': 'numero_correlativo',
    'Tipo Doc Identidad': 'tipo_documento',
    'Nro Doc Identidad': 'numero_documento',
    'BI Gravado DG': 'bi_gravadas',
    'IGV / IPM DG': 'igv_gravadas',
    'BI Gravado DGNG': 'bi_gravadas_nogravadas',
    'IGV / IPM DGNG': 'igv_gravadas_nogravadas',
    'BI Gravado DNG': 'bi_nogravadas',
    'IGV / IPM DNG': 'igv_nogravadas',
    'Valor Adq. NG': 'no_gravadas',
    'ISC': 'isc',
    'ICBPER': 'icbp',
    'Otros Trib/ Cargos': 'otros',
    'Moneda': 'tipo_moneda',
    'Tipo CP Modificado': 'tipo_comprobante_modificado',
    'Serie CP Modificado': 'numero_serie_modificado',
    'Nro CP Modificado': 'numero_correlativo_modificado',
    'Detracción': 'tasa_detraccion',
})

# Usamos .copy() para evitar warnings de Pandas
df_transformado = df.copy()

# 1. PREPARACIÓN: Asegurarse de que TODAS las columnas de valor sean numéricas
columnas_valor = [
    'Base imponible 1', 'IGV 1',
    'Base imponible 2', 'IGV 2',
    'Base imponible 3', 'IGV 3',
    'Valor no gravado',
    'Otros cargos'  # <-- AÑADIMOS la columna de otros cargos
]

for col in columnas_valor:
    # Si la columna no existe, la creamos y la llenamos con 0
    if col not in df_transformado.columns:
        df_transformado[col] = 0
    df_transformado[col] = pd.to_numeric(df_transformado[col], errors='coerce').fillna(0)

# 2. DEFINIR CONDICIONES (sin cambios)
cond_destino_5 = (
                         (df_transformado['Base imponible 1'] > 0) |
                         (df_transformado['Base imponible 2'] > 0) |
                         (df_transformado['Base imponible 3'] > 0)
                 ) & (df_transformado['Valor no gravado'] > 0)
cond_destino_1 = (df_transformado['Base imponible 1'] > 0)
cond_destino_2 = (df_transformado['Base imponible 2'] > 0)
cond_destino_3 = (df_transformado['Base imponible 3'] > 0)
cond_destino_4 = (df_transformado['Valor no gravado'] > 0)

condiciones = [
    cond_destino_5, cond_destino_1, cond_destino_2,
    cond_destino_3, cond_destino_4
]
# 3. DEFINIR RESULTADOS: Listas paralelas para cada columna nueva

# Resultados para la columna 'destino'
resultados_destino = [5, 1, 2, 3, 4]

# Resultados para la columna 'valor'
resultados_valor = [
    df_transformado['Base imponible 1'] + df_transformado['Base imponible 2'] + df_transformado['Base imponible 3'],
    # Destino 5
    df_transformado['Base imponible 1'],  # Destino 1
    df_transformado['Base imponible 2'],  # Destino 2
    df_transformado['Base imponible 3'],  # Destino 3
    df_transformado['Valor no gravado']  # Destino 4
]

# Resultados para la columna 'igv'
resultados_igv = [
    df_transformado['IGV 1'] + df_transformado['IGV 2'] + df_transformado['IGV 3'],  # Destino 5
    df_transformado['IGV 1'],  # Destino 1
    df_transformado['IGV 2'],  # Destino 2
    df_transformado['IGV 3'],  # Destino 3
    0  # Destino 4
]

# Resultados para la columna 'otros' (CON LA LÓGICA DE SUMA)
resultados_otros = [
    df_transformado['Otros cargos'] + df_transformado['Valor no gravado'],  # <-- CAMBIO CLAVE: Sumamos los valores
    df_transformado['Otros cargos'],  # Si es destino 1, 2, 3 o 4, mantenemos los otros cargos originales
    df_transformado['Otros cargos'],
    df_transformado['Otros cargos'],
    df_transformado['Otros cargos']
]

# 4. APLICAR LAS REGLAS con np.select
df_transformado['destino'] = np.select(condiciones, [5, 1, 2, 3, 4], default=0)
df_transformado['valor'] = np.select(condiciones, resultados_valor, default=0)
df_transformado['igv'] = np.select(condiciones, resultados_igv, default=0)
df_transformado['otros'] = np.select(condiciones, resultados_otros, default=0)


# --- Cambiar tipos de datos ---
# Convertir texto a fechas (ajusta el formato si es necesario)
df['periodo_tributario'] = pd.to_datetime(df['periodo_tributario'], format='%Y%m')
# Convertir texto a números, manejando errores
df['igv'] = pd.to_numeric(df['igv'], errors='coerce').fillna(0)
df['isc'] = pd.to_numeric(df['isc'], errors='coerce').fillna(0)
df['icbp'] = pd.to_numeric(df['icbp'], errors='coerce').fillna(0)
df['otros'] = pd.to_numeric(df['otros'], errors='coerce').fillna(0)

# --- Modificar/Crear columnas ---
# Unir dos columnas en una nueva (ejemplo hipotético)
# df['info_completa'] = df['columna_A'] + ' - ' + df['columna_B']

# Asegurar el formato con 2 decimales
# df['igv'] = df['igv'].round(2)

print(df)
from sqlalchemy import create_engine

"""
# Crea el motor de conexión (usando tus variables de .env)
db_url = "postgresql://usuario:contraseña@host:puerto/basededatos"
engine = create_engine(db_url)

# Carga los datos del DataFrame en la tabla 'acc._5'
# 'if_exists='append'' añade los nuevos registros sin borrar los existentes.
# 'index=False' evita que se inserte una columna de índice de Pandas.
df.to_sql('_5', engine, schema='acc', if_exists='append', index=False)
"""
"""
def procesar_zip_a_db(ruta_zip, db_params, tabla_db):

    Procesa un archivo ZIP, carga su contenido (CSV o TXT) en un DataFrame,
    lo inserta en una base de datos PostgreSQL y muestra un resumen.

    Args:
        ruta_zip (str): La ruta completa al archivo .zip.
        db_params (dict): Un diccionario con los parámetros de conexión a la base de datos.
        tabla_db (str): El nombre de la tabla en la base de datos donde se insertarán los datos.

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
###TOMAR EN CUENTA CUANDO EL COMPROBANTE ES TIPO 30 TIENE UN IGV MENOR A 1%, ESTE Y CASOS SIMILARES
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
    ruta_del_zip = 'C:/Users/Raknaros/Downloads/20548030529-20250918-224152-propuesta.zip'

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
