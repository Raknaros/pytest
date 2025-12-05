import os
import zipfile
import pandas as pd
import io
from typing import List, Dict

# Configurar pandas para mostrar todo (opcional, para depuración)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def procesar_reportes_zip(rutas_zip: List[str]) -> Dict[str, pd.DataFrame]:
    """
    Procesa una lista de archivos ZIP que contienen reportes de planilla (TRA, IDE, SSA).

    Extrae, transforma y agrupa los datos en un diccionario de DataFrames,
    uno para cada tipo de reporte.
    """

    # --- 1. Preparación ---
    # Diccionarios para guardar los datos y las cabeceras de cada reporte
    datos_por_reporte = {
        "TRA": [],
        "IDE": [],
        "SSA": []
        # Puedes añadir más tipos de reporte aquí
    }
    cabeceras_por_reporte = {}

    print(f"Iniciando procesamiento de {len(rutas_zip)} archivo(s) ZIP...")

    # --- 2. Bucle Principal (Extraer y Transformar) ---
    for zip_file_path in rutas_zip:
        if not os.path.exists(zip_file_path):
            print(f"⚠️ Advertencia: No se encontró el archivo ZIP en {zip_file_path}. Saltando.")
            continue

        try:
            with zipfile.ZipFile(zip_file_path, 'r') as z:
                for file_name in z.namelist():

                    # Extraer el tipo de reporte (ej. "TRA") del nombre del archivo
                    try:
                        reporte_tipo = file_name[12:15]
                    except IndexError:
                        continue  # El nombre del archivo es muy corto, saltar

                    # Si no es un tipo de reporte que nos interesa, lo ignoramos
                    if reporte_tipo not in datos_por_reporte:
                        continue

                    print(f"  Procesando archivo: {file_name} (Tipo: {reporte_tipo})")

                    # Usamos io.TextIOWrapper para leer el archivo como texto (no bytes)
                    # Esto evita tener que usar .decode() en cada línea
                    with io.TextIOWrapper(z.open(file_name, 'r'), encoding='latin-1') as txt_file:

                        filas_de_datos = []
                        ruc = None
                        timestamp = None

                        for index, line in enumerate(txt_file):

                            # Requisito: Capturar RUC (Línea 3, índice 2)
                            if index == 2:
                                try:
                                    ruc = line.split(':', 1)[-1].strip()
                                except Exception:
                                    ruc = None  # Error de formato

                            # Requisito: Capturar Timestamp (Línea 5, índice 4)
                            elif index == 4:
                                try:
                                    ts_str = line.split(':', 1)[-1].strip()
                                    timestamp = pd.to_datetime(ts_str, format='%d/%m/%Y %H:%M:%S')
                                except Exception:
                                    timestamp = None  # Error de formato

                            # Requisito: Capturar Cabecera (Línea 10, índice 9)
                            elif index == 9:
                                cabecera = [h.strip() for h in line.split('|')]
                                # Guardamos la cabecera (columnas de datos)
                                if reporte_tipo not in cabeceras_por_reporte:
                                    cabeceras_por_reporte[reporte_tipo] = cabecera

                            # Requisito: Capturar Filas de Datos (Línea 12 en adelante)
                            elif index >= 11:
                                cleaned_line = line.strip()
                                if cleaned_line:  # Ignorar líneas vacías
                                    filas_de_datos.append([e.strip() for e in cleaned_line.split('|')])

                        # --- Fin del bucle del archivo ---
                        # Ahora, añadir las columnas de metadatos a todas las filas que encontramos
                        for fila in filas_de_datos:
                            fila.extend([ruc, timestamp, reporte_tipo])

                        # Guardamos las filas procesadas en nuestro diccionario principal
                        datos_por_reporte[reporte_tipo].extend(filas_de_datos)

        except zipfile.BadZipFile:
            print(f"❌ Error: El archivo {zip_file_path} está corrupto o no es un ZIP.")
        except Exception as e:
            print(f"❌ Error inesperado procesando {zip_file_path}: {e}")

    # --- 3. Carga (Crear los DataFrames) ---
    # Este paso se hace UNA SOLA VEZ, al final de todos los bucles.
    print("...Procesamiento de archivos finalizado. Creando DataFrames...")

    dataframes_finales = {}

    for reporte_tipo, filas in datos_por_reporte.items():
        if not filas:
            print(f"ℹ️ No se encontraron datos para el reporte tipo '{reporte_tipo}'.")
            continue

        try:
            # Construir la lista final de columnas
            columnas_base = cabeceras_por_reporte.get(reporte_tipo, [])
            if not columnas_base:
                print(
                    f"❌ Error: No se encontró cabecera para el reporte tipo '{reporte_tipo}'. No se puede crear DataFrame.")
                continue

            columnas_metadata = ['ruc', 'timestamp', 'reporte_tipo']
            columnas_totales = columnas_base + columnas_metadata

            # Crear el DataFrame de una sola vez
            df = pd.DataFrame(filas, columns=columnas_totales)
            dataframes_finales[reporte_tipo] = df

            print(f"✅ DataFrame para '{reporte_tipo}' creado exitosamente ({len(df)} filas).")

        except Exception as e:
            print(f"❌ Error al crear el DataFrame para '{reporte_tipo}': {e}")

    return dataframes_finales


def cargar_reportes_a_db(reportes: Dict[str, pd.DataFrame], engine):
    """
    Toma el diccionario de DataFrames, los renombra según una configuración
    y los carga en la base de datos.
    """
    mapeo_tra = {
        "Tipo Doc": "tipo_documento_id",
        "Nro Doc": "numero_documento",
        "ApePat": "apellido_paterno",
        "ApeMat": "apellido_materno",
        "Nombres": "nombres",
        "FecNac": "fecha_nacimiento",
        # ... (añade todas las columnas de tu cabecera de 'TRA')

        # ¡IMPORTANTE! No olvides mapear las columnas que añadimos en el paso anterior
        "ruc": "ruc_empresa",
        "timestamp": "fecha_reporte"
        # Ignoramos 'reporte_tipo' ya que no necesitamos guardarlo
    }

    # Mapeo para las columnas de los archivos "IDE"
    mapeo_ide = {
        "Tipo Doc": "tipo_documento_id",
        "Nro Doc": "numero_documento",
        "Fec Ini Lab": "fecha_inicio_laboral",
        # ... (etc.)
        "ruc": "ruc_empresa",
        "timestamp": "fecha_reporte"
    }

    mapeo_ssa = {
        "Tipo Doc": "tipo_documento_id",
        "Nro Doc": "numero_documento",
        "Fec Ini Lab": "fecha_inicio_laboral",
        # ... (etc.)
        "ruc": "ruc_empresa",
        "timestamp": "fecha_reporte"
    }
    # ... (Crea un mapeo para 'SSA' y otros)

    # --- 2. CONFIGURACIÓN PRINCIPAL DE CARGA ---
    # Esto le dice al script a qué tabla y esquema va cada reporte
    configuracion_de_carga = {
        "TRA": {
            "tabla_destino": "tra",  # Nombre de tu tabla SQL
            "schema_destino": "payroll",  # Nombre de tu esquema SQL
            "mapeo_columnas": mapeo_tra
        },
        "IDE": {
            "tabla_destino": "ide",
            "schema_destino": "payroll",
            "mapeo_columnas": mapeo_ide
        },
        "SSA": {
            "tabla_destino": "ssa",
            "schema_destino": "payroll",
            "mapeo_columnas": mapeo_ssa
        }
        # ... (etc.)
    }

    # --- 3. BUCLE DE PROCESAMIENTO Y CARGA ---
    print("\n--- Iniciando fase de Carga (Load) a la Base de Datos ---")

    # Itera sobre los DataFrames que generaste (ej. 'TRA', 'IDE', ...)
    for reporte_tipo, df in reportes.items():

        # Revisa si tenemos una configuración para este tipo de reporte
        if reporte_tipo not in configuracion_de_carga:
            print(f"⚠️ Advertencia: No se encontró configuración de carga para el reporte '{reporte_tipo}'. Saltando.")
            continue

        config = configuracion_de_carga[reporte_tipo]
        print(
            f"Procesando reporte '{reporte_tipo}' para la tabla '{config['schema_destino']}.{config['tabla_destino']}'...")

        try:
            # --- Renombrar Columnas ---
            df_renombrado = df.rename(columns=config['mapeo_columnas'])

            # --- Seleccionar Columnas ---
            # Nos aseguramos de que solo se envíen a la BD las columnas que
            # hemos definido en el mapeo, y en ese orden.
            columnas_sql = list(config['mapeo_columnas'].values())
            df_para_cargar = df_renombrado[columnas_sql]

            # --- Cargar a la Base de Datos ---
            df_para_cargar.to_sql(
                name=config['tabla_destino'],
                con=engine,
                schema=config['schema_destino'],
                if_exists='append',  # 'append' = añadir nuevos. 'replace' = borrar y reemplazar.
                index=False  # No insertar el índice de Pandas como una columna
            )
            print(f"✅ Éxito: Se cargaron {len(df_para_cargar)} filas en '{config['tabla_destino']}'.")

        except KeyError as e:
            print(
                f"❌ Error de Mapeo en '{reporte_tipo}': No se encontró la columna {e} en el DataFrame. Revisa tu mapeo.")
        except Exception as e:
            print(f"❌ Error Inesperado al cargar '{reporte_tipo}': {e}")

# --- Ejemplo de Uso ---
if __name__ == "__main__":

    # Simula una lista de rutas a archivos ZIP
    lista_de_zips = [
        "C:/Users/Raknaros/Downloads/20606283858_SSA_21102025.zip",
    ]

    # Ejecutar la función principal
    reportes = procesar_reportes_zip(lista_de_zips)

    cargar_reportes_a_db(reportes, engine)

#1 trabajador, 2 pensionista, 4 personal de terceros, 5 personal en formacion



"""
reporte = 'C:/Users/Raknaros/Downloads/test_planillas.xlsx'
reporte_df = pd.read_excel(reporte, parse_dates=['   Fec. Inicio   '], dtype={'    Número    ': str, 'Discapacidad': bool, 'Sindicalizado': bool, 'Reg. Acumulativo': bool, 'Máxima': bool, 'Horario Nocturno': bool})#
reporte_df.drop(columns=['Column1'], inplace=True)
reporte_df.columns = ['tipo_documento', 'numero_documento', 'apellido_paterno', 'apellido_materno', 'nombre', 'fecha_inicio', 'tipo_trabajador', 'regimen_laboral', 'cat_ocu', 'ocupacion', 'nivel_educativo', 'discapacidad', 'sindicalizado', 'regimen_alt', 'jornada_maxima', 'horario_nocturno', 'situacion_especial', 'establecimiento', 'tipo_contrato', 'tipo_pago', 'periodicidad', 'entidad_financiera', 'nro_cuenta', 'remuneracion', 'situacion', 'reporte', 'ruc']
reporte_df['reporte'] = reporte_df['reporte'].replace({'TRA': '1', 'OTRO': 10})
reporte_df['tipo_documento'] = reporte_df['tipo_documento'].replace({'L.E / DNI': '01', 'OTRO': 10})
reporte_df['cui'] = reporte_df.apply(lambda row: str(hex(row['ruc']) + str(hex(int(row['reporte'] + row['tipo_documento'] + str(row['numero_documento']))))[2:])[2:].upper(), axis=1)
datos_identificacion = reporte_df[['ruc', 'tipo_documento', 'numero_documento', 'apellido_paterno', 'apellido_materno', 'nombre', 'cui']]
trabajador = reporte_df[['fecha_inicio', 'tipo_trabajador', 'regimen_laboral', 'cat_ocu', 'ocupacion', 'nivel_educativo', 'discapacidad', 'sindicalizado', 'regimen_alt', 'jornada_maxima', 'horario_nocturno', 'situacion_especial', 'establecimiento', 'tipo_contrato', 'tipo_pago', 'periodicidad', 'entidad_financiera', 'nro_cuenta', 'remuneracion', 'situacion']]
trabajador['cui_relacionado'] = reporte_df['cui']
#datos_identificacion.to_sql(name='ide', con=warehouse, if_exists='append', index=False, schema='payroll')
trabajador.to_sql(name='tra', con=warehouse, if_exists='append', index=False, schema='payroll')
"""