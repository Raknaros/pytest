import os
import zipfile
import logging
import numpy as np
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
from sqlalchemy import create_engine
from typing import List, Optional


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# Configuración de logging
logging.basicConfig(
    filename='etl_sire.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)


class Extractor:
    """Clase responsable de extraer datos de archivos CSV, TXT y ZIP."""

    @staticmethod
    def extract_files(rutas_archivos: List[str]) -> List[pd.DataFrame]:
        """
        Extrae datos de múltiples archivos. Soporta CSV, TXT y ZIP.
        Para ZIP, descomprime en memoria y procesa archivos internos.
        """
        lista_dataframes = []
        logging.info("Iniciando fase de extracción")

        for ruta in rutas_archivos:
            try:
                if not os.path.exists(ruta):
                    logging.error(f"Archivo no encontrado: {ruta}")
                    continue

                logging.info(f"Procesando archivo: {ruta}")

                if ruta.lower().endswith('.zip'):
                    # Descomprimir en memoria
                    with zipfile.ZipFile(ruta, 'r') as zip_ref:
                        for nombre_archivo in zip_ref.namelist():
                            if nombre_archivo.lower().endswith(('.csv', '.txt')):
                                with zip_ref.open(nombre_archivo) as file:
                                    content = file.read().decode('utf-8')
                                    sep = ',' if nombre_archivo.lower().endswith('.csv') else '|'
                                    df = pd.read_csv(StringIO(content), sep=sep, header=0, dtype=str)
                                    lista_dataframes.append(df)
                                    logging.info(f"Extraído archivo interno: {nombre_archivo}")
                elif ruta.lower().endswith('.csv'):
                    df = pd.read_csv(ruta, sep=',', header=0, dtype=str)
                    lista_dataframes.append(df)
                elif ruta.lower().endswith('.txt'):
                    df = pd.read_csv(ruta, sep='|', header=0, dtype=str)
                    lista_dataframes.append(df)
                else:
                    logging.warning(f"Formato no soportado: {ruta}")

            except Exception as e:
                logging.error(f"Error al procesar {ruta}: {str(e)}")

        if not lista_dataframes:
            logging.warning("No se cargaron datos válidos")
            return []

        # Concatenar todos los dataframes
        df_completo = pd.concat(lista_dataframes, ignore_index=True)
        logging.info(f"Datos extraídos: {len(df_completo)} filas")
        return [df_completo]  # Retornar como lista para consistencia


class Transformer:
    """Clase responsable de transformar y filtrar los datos."""

    @staticmethod
    def transform_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica transformaciones: filtros, conversiones de tipos, normalización.
        Incluye el filtro complejo para crear columnas destino, valor, igv, otros.
        """
        logging.info("Iniciando fase de transformación")
        df_transformado = df.copy()

        # Filtro CAR SUNAT: eliminar filas donde no tenga exactamente 27 caracteres
        if 'CAR SUNAT' in df_transformado.columns:
            before_count = len(df_transformado)
            df_transformado = df_transformado[df_transformado['CAR SUNAT'].str.len() == 27]
            after_count = len(df_transformado)
            logging.info(f"Filtro CAR SUNAT: {before_count} -> {after_count} filas")

        # Regla especial para Tipo Doc Identidad y Nro Doc Identidad
        if 'Tipo Doc Identidad' in df_transformado.columns and 'Nro Doc Identidad' in df_transformado.columns and 'Apellidos Nombres/ Razón Social' in df_transformado.columns:
            # Si 'Tipo Doc Identidad' es '-', cambiar a '0'
            tipo_doc_mask = df_transformado['Tipo Doc Identidad'] == '-'
            df_transformado.loc[tipo_doc_mask, 'Tipo Doc Identidad'] = '0'

            # Si 'Tipo Doc Identidad' es '0' y 'Nro Doc Identidad' es '-', reemplazar con 'Apellidos Nombres/ Razón Social'
            nro_doc_mask = (df_transformado['Tipo Doc Identidad'] == '0') & (df_transformado['Nro Doc Identidad'] == '-')
            df_transformado.loc[nro_doc_mask, 'Nro Doc Identidad'] = df_transformado.loc[nro_doc_mask, 'Apellidos Nombres/ Razón Social']

            modified_count = tipo_doc_mask.sum() + nro_doc_mask.sum()
            logging.info(f"Regla especial documentos: {modified_count} filas modificadas")

        # Normalización de fechas
        if 'Fecha de emisión' in df_transformado.columns:
            df_transformado['Fecha de emisión'] = pd.to_datetime(df_transformado['Fecha de emisión'],
                                                                 format='%d/%m/%Y', errors='coerce').dt.date
        if 'Fecha Vcto/Pago' in df_transformado.columns:
            df_transformado['Fecha Vcto/Pago'] = pd.to_datetime(df_transformado['Fecha Vcto/Pago'],
                                                                 format='%d/%m/%Y', errors='coerce').dt.date

        # Transformación específica del Periodo
        if 'Periodo' in df_transformado.columns:
            df_transformado['Periodo'] = pd.to_datetime(df_transformado['Periodo'], format='%Y%m', errors='coerce')

        # Normalización de montos (asumiendo columnas numéricas)
        columnas_monto = ['BI Gravada', 'Dscto BI', 'IGV / IPM', 'Dscto IGV / IPM',
                          'Mto Exonerado', 'Mto Inafecto', 'BI Grav IVAP', 'IVAP',
                          'ISC', 'ICBPER', 'Otros Tributos']
        for col in columnas_monto:
            if col in df_transformado.columns:
                df_transformado[col] = pd.to_numeric(df_transformado[col], errors='coerce').fillna(0)

        if 'Tipo Doc Identidad' in df_transformado.columns:
            df_transformado['Tipo Doc Identidad'] = df_transformado['Tipo Doc Identidad'].replace('-', 0)
            df_transformado['Tipo Doc Identidad'] = pd.to_numeric(df_transformado['Tipo Doc Identidad'], errors='coerce')


        # Aplicar filtro complejo para crear nuevas columnas
        Transformer._aplicar_filtro_complejo(df_transformado)

        logging.info(f"Transformación completada: {len(df_transformado)} filas")
        return df_transformado

    @staticmethod
    def _aplicar_filtro_complejo(df: pd.DataFrame) -> None:
        """Aplica el filtro complejo para crear columnas destino, valor, igv, otros."""
        # 1. PREPARACIÓN: Asegurarse de que TODAS las columnas de valor sean numéricas
        columnas_valor = [
            'BI Gravada', 'Dscto BI', 'IGV / IPM', 'Dscto IGV / IPM',
            'Mto Exonerado', 'Mto Inafecto', 'BI Grav IVAP', 'IVAP',
            'Otros Tributos', 'Valor Facturado Exportación', 'Tipo CP/Doc.'
        ]

        for col in columnas_valor:
            if col not in df.columns:
                df[col] = 0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 2. DEFINIR CONDICIONES
        # -- Lógica de Negocio con np.select (ORDEN DE PRIORIDAD CORRECTO) --
        # Usando los nombres de columna originales

        # Pre-calcular sumas para legibilidad (usando nombres originales)
        suma_exo_inaf = df['Mto Exonerado'] + df['Mto Inafecto']

        condiciones = [
            # 1. Regla B: Nota Crédito Exportación (Tipo 7, Export < 0)
            (df['Tipo CP/Doc.'] == 7) & (df['Valor Facturado Exportación'] < 0),
            # 2. Regla A: Nota Crédito Interna (Tipo 7, Export == 0)
            (df['Tipo CP/Doc.'] == 7) & (df['Valor Facturado Exportación'] == 0),
            # 3. Regla 1: Exportación (Tipo <> 7, Export > 0, resto BI/IGV/IVAP = 0)
            (df['Tipo CP/Doc.'] != 7) & (df['Valor Facturado Exportación'] > 0) & (df['BI Gravada'] == 0) & (
                        df['Dscto BI'] == 0) & (df['IGV / IPM'] == 0) & (df['Dscto IGV / IPM'] == 0) & (
                        df['Mto Exonerado'] == 0) & (df['Mto Inafecto'] == 0) & (df['BI Grav IVAP'] == 0) & (
                        df['IVAP'] == 0),
            # 4. Regla 3: Gravada con Exo/Inaf (Tipo <> 7, BI Grav > 0, IGV > 0, Exo/Inaf > 0)
            (df['Tipo CP/Doc.'] != 7) & (df['Valor Facturado Exportación'] == 0) & (df['BI Gravada'] > 0) & (
                        df['IGV / IPM'] > 0) & (suma_exo_inaf > 0) & (df['BI Grav IVAP'] == 0) & (
                        df['IVAP'] == 0),
            # 5. Regla 2: Solo Gravada (Tipo <> 7, BI Grav > 0, IGV > 0, Exo/Inaf = 0)
            (df['Tipo CP/Doc.'] != 7) & (df['Valor Facturado Exportación'] == 0) & (df['BI Gravada'] > 0) & (
                        df['IGV / IPM'] > 0) & (suma_exo_inaf == 0) & (df['BI Grav IVAP'] == 0) & (
                        df['IVAP'] == 0),
            # 6. Regla C: Solo Exo/Inaf (Tipo <> 7, Exo/Inaf > 0, resto BI/IGV/IVAP = 0)
            (df['Tipo CP/Doc.'] != 7) & (df['Valor Facturado Exportación'] == 0) & (df['BI Gravada'] == 0) & (
                        df['Dscto BI'] == 0) & (df['IGV / IPM'] == 0) & (df['Dscto IGV / IPM'] == 0) & (
                        suma_exo_inaf > 0) & (df['BI Grav IVAP'] == 0) & (df['IVAP'] == 0),
            # 7. Regla 6: Solo IVAP (Tipo <> 7, BI IVAP > 0, IVAP > 0, resto BI/IGV/Exo/Inaf = 0)
            (df['Tipo CP/Doc.'] != 7) & (df['Valor Facturado Exportación'] == 0) & (df['BI Gravada'] == 0) & (
                        df['Dscto BI'] == 0) & (df['IGV / IPM'] == 0) & (df['Dscto IGV / IPM'] == 0) & (
                        suma_exo_inaf == 0) & (df['BI Grav IVAP'] > 0) & (df['IVAP'] > 0)
        ]

        # Resultados para cada columna destino (usando nombres originales)
        resultados_tipo_op = [1, 1, 17, 1, 1, 1, 1]
        resultados_destino = [1, 1, 2, 3, 1, 2, 4]  # Usando destino=2 para Exo/Inaf

        resultados_valor = [
            df['BI Gravada'] + df['Dscto BI'] + df['BI Grav IVAP'],  # NC Interna
            df['Valor Facturado Exportación'],  # NC Exportación
            df['Valor Facturado Exportación'],  # Exportación
            df['BI Gravada'],  # Gravada con Exo/Inaf
            df['BI Gravada'],  # Solo Gravada
            suma_exo_inaf,  # Solo Exo/Inaf
            df['BI Grav IVAP']  # Solo IVAP
        ]

        resultados_igv = [
            df['IGV / IPM'] + df['Dscto IGV / IPM'] + df['IVAP'],  # NC Interna
            0,  # NC Exportación
            0,  # Exportación
            df['IGV / IPM'],  # Gravada con Exo/Inaf
            df['IGV / IPM'],  # Solo Gravada
            0,  # Solo Exo/Inaf
            df['IVAP']  # Solo IVAP
        ]

        resultados_otros = [
            df['Otros Tributos'],  # NC Interna
            df['Otros Tributos'],  # NC Exportación
            df['Otros Tributos'],  # Exportación
            df['Otros Tributos'] + suma_exo_inaf,  # Gravada con Exo/Inaf
            df['Otros Tributos'],  # Solo Gravada
            df['Otros Tributos'],  # Solo Exo/Inaf
            df['Otros Tributos'] + suma_exo_inaf  # Solo IVAP
        ]

        # Aplicar con np.select, usando 99 como default para detectar no clasificados
        default_tipo_op = 99
        default_destino = 99

        df['tipo_operacion'] = np.select(condiciones, resultados_tipo_op, default=default_tipo_op)
        df['destino'] = np.select(condiciones, resultados_destino, default=default_destino)
        df['valor'] = np.select(condiciones, resultados_valor, default=0)
        df['igv'] = np.select(condiciones, resultados_igv, default=0)
        df['otros_cargos'] = np.select(condiciones, resultados_otros,
                                        default=df['Otros Tributos'])  # Default mantiene 'Otros Tributos'

        # Añadir observación si no se clasificó
        # Asegúrate de que la columna 'observaciones' exista antes de esta línea.
        # Si la creas antes con el CAR SUNAT, está bien.
        df.loc[df['destino'] == default_destino, 'CAR SUNAT'] = df['CAR SUNAT'].astype(
            str) + " | Revisar dinamica de destino"
        print("✅ Lógica de negocio aplicada.")
        print("✅ Lógica de negocio aplicada.")

    @staticmethod
    def rename_columns(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
        """Cambia los nombres de las columnas según el mapping proporcionado."""
        df_renamed = df.rename(columns=mapping)
        # Crear columna observaciones: "SIRE" + CAR SUNAT
        if 'observaciones' in df_renamed.columns:
            df_renamed['observaciones'] = "SIRE:" + df_renamed['observaciones']
        return df_renamed

    @staticmethod
    def filter_final_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Filtra solo las columnas finales que van a la base de datos según el esquema."""
        columnas_finales = [
            'ruc', 'periodo_tributario', 'tipo_comprobante', 'fecha_emision',
            'fecha_vencimiento', 'numero_serie', 'numero_correlativo', 'numero_final', 'tipo_documento',
            'numero_documento', 'destino', 'valor', 'igv', 'icbp', 'isc', 'otros_cargos',
            'tipo_moneda', 'tipo_comprobante_modificado','numero_serie_modificado',
            'numero_correlativo_modificado', 'observaciones'
        ]

        # Filtrar solo las columnas que existen
        columnas_existentes = [col for col in columnas_finales if col in df.columns]
        df_filtrado = df[columnas_existentes].copy()

        # Convertir valores vacíos a NaN (que será NULL en PostgreSQL)
        df_filtrado = df_filtrado.replace('', np.nan)
        df_filtrado = df_filtrado.replace(' ', np.nan)

        # Convertir tipos de datos según esquema PostgreSQL
        Transformer._convert_data_types(df_filtrado)

        return df_filtrado

    @staticmethod
    def _convert_data_types(df: pd.DataFrame) -> None:
        """Convierte los tipos de datos según el esquema PostgreSQL."""
        # bigint
        if 'ruc' in df.columns:
            df['ruc'] = pd.to_numeric(df['ruc'], errors='coerce').astype('Int64')

        # integer
        int_columns = ['periodo_tributario', 'tipo_comprobante', 'destino', 'tasa_detraccion',
                       'tipo_comprobante_modificado', 'numero_final']
        for col in int_columns:
            if col in df.columns:
                if col == 'periodo_tributario':
                    # Convertir periodo a formato YYYYMM
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y%m').astype('Int64')
                else:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        # date
        date_columns = ['fecha_emision', 'fecha_vencimiento']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        # varchar
        varchar_columns = ['numero_serie', 'numero_correlativo', 'tipo_documento', 'numero_documento',
                           'tipo_moneda', 'numero_serie_modificado', 'numero_correlativo_modificado', 'observaciones']
        for col in varchar_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).replace('nan', np.nan)

        # numeric con precisión específica
        if 'valor' in df.columns:
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce').round(2)
        if 'igv' in df.columns:
            df['igv'] = pd.to_numeric(df['igv'], errors='coerce').round(2)
        if 'icbp' in df.columns:
            df['icbp'] = pd.to_numeric(df['icbp'], errors='coerce').round(2)
        if 'isc' in df.columns:
            df['isc'] = pd.to_numeric(df['isc'], errors='coerce').round(2)
        if 'otros_cargos' in df.columns:
            df['otros_cargos'] = pd.to_numeric(df['otros_cargos'], errors='coerce').round(2)


class Loader:
    """Clase responsable de cargar datos a PostgreSQL."""

    def __init__(self, db_url: str, schema: str, table: str):
        self.engine = create_engine(db_url)
        self.schema = schema
        self.table = table

    def load_data(self, df: pd.DataFrame) -> bool:
        """
        Carga el DataFrame a la tabla PostgreSQL.
        Retorna True si exitoso, False en caso contrario.
        """
        logging.info("Iniciando fase de carga")
        try:
            full_table_name = f"{self.schema}.{self.table}"
            df.to_sql(self.table, self.engine, schema=self.schema, if_exists='append', index=False)
            logging.info(f"Datos cargados exitosamente: {len(df)} filas a {full_table_name}")
            return True
        except Exception as e:
            logging.error(f"Error al cargar datos: {str(e)}")
            return False


class ETLSIRE:
    """Clase principal que orquesta el proceso ETL."""

    def __init__(self, db_url: str, schema: str, table: str, column_mapping: Optional[dict] = None):
        self.extractor = Extractor()
        self.transformer = Transformer()
        self.loader = Loader(db_url, schema, table)
        self.column_mapping = column_mapping or {}

    def run_etl(self, rutas_archivos: List[str], show_preview: bool = False) -> bool:
        """
        Ejecuta el proceso ETL completo.
        Retorna True si exitoso, False en caso contrario.
        """
        try:
            # Extract
            dataframes = self.extractor.extract_files(rutas_archivos)
            if not dataframes:
                return False
            df = dataframes[0]  # Asumimos un dataframe consolidado

            # Transform
            df = self.transformer.transform_data(df)

            # Rename columns if mapping provided
            if self.column_mapping:
                df = self.transformer.rename_columns(df, self.column_mapping)

            # Filtrar columnas finales y convertir tipos
            df = self.transformer.filter_final_columns(df)

            # Mostrar preview si se solicita
            if show_preview:
                print("=== PREVIEW DEL DATAFRAME FINAL ===")
                print(f"Shape: {df.shape}")
                print(f"Columnas: {list(df.columns)}")
                print("\nPrimeras 5 filas:")
                print(df.head())
                print("\nInformación de tipos de datos:")
                print(df.dtypes)
                print("\nEstadísticas descriptivas:")
                print(df.describe(include='all'))
                print("=" * 50)

            # Load
            success = self.loader.load_data(df)
            return success

        except Exception as e:
            logging.error(f"Error en el proceso ETL: {str(e)}")
            return False


# Función principal para ejecutar el ETL
def main():
    load_dotenv()

    # Configuración
    rutas_archivos = [
        "C:/Users/Raknaros/Downloads/LE2061455137320251010014040001EXP2.zip",
        "C:/Users/Raknaros/Downloads/LE1072650130620250410014040001EXP2.zip",
        "C:/Users/Raknaros/Downloads/LE2060648275320250410014040001EXP2.zip"
    ]
    db_url = f"postgresql://{os.getenv('DB_WAREHOUSE_USER')}:{os.getenv('DB_WAREHOUSE_PASSWORD')}@{os.getenv('DB_WAREHOUSE_HOST')}:{os.getenv('DB_WAREHOUSE_PORT')}/{os.getenv('DB_WAREHOUSEE_NAME')}"
    schema = "acc"
    table = "_5"

    # Mapping de columnas según especificación
    column_mapping = {
        'Ruc': 'ruc',
        'Periodo': 'periodo_tributario',
        'CAR SUNAT': 'observaciones',
        'Fecha de emisión': 'fecha_emision',
        'Fecha Vcto/Pago': 'fecha_vencimiento',
        'Tipo CP/Doc.': 'tipo_comprobante',
        'Serie del CDP': 'numero_serie',
        'Nro CP o Doc. Nro Inicial (Rango)': 'numero_correlativo',
        'Nro Final (Rango)': 'numero_final',
        'Tipo Doc Identidad': 'tipo_documento',
        'Nro Doc Identidad': 'numero_documento',
        'ISC': 'isc',
        'ICBPER': 'icbp',
        'Moneda': 'tipo_moneda',
        'Tipo CP Modificado': 'tipo_comprobante_modificado',
        'Serie CP Modificado': 'numero_serie_modificado',
        'Nro CP Modificado': 'numero_correlativo_modificado',
        # Mantener las columnas del filtro complejo
        'destino': 'destino',
        'valor': 'valor',
        'igv': 'igv',
        'otros_cargos': 'otros_cargos'
    }

    # Ejecutar ETL con preview
    etl = ETLSIRE(db_url, schema, table, column_mapping)
    success = etl.run_etl(rutas_archivos, show_preview=True)

    if success:
        print("ETL completado exitosamente")
    else:
        print("ETL falló - revisar logs")


if __name__ == "__main__":
    main()
