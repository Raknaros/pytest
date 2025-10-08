import os
import zipfile
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


def etl_sire_compras(ruta_zip: str, db_url: str, schema_db: str, tabla_db: str) -> pd.DataFrame | None:
    """
    Proceso ETL completo para leer un ZIP del SIRE, transformar los datos
    y cargarlos a una base de datos PostgreSQL.
    Devuelve el DataFrame final si tiene éxito, o None si falla.
    """
    try:
        # --- 1. EXTRAER ---
        with zipfile.ZipFile(ruta_zip, 'r') as z:
            nombre_archivo = z.namelist()[0]
            with z.open(nombre_archivo) as archivo:
                df = pd.read_csv(archivo, sep='|', header=0, dtype=str)
        print(f"✅ Archivo '{nombre_archivo}' extraído y leído. {len(df)} filas encontradas.")

        # --- 2. TRANSFORMAR ---

        # -- Renombrar Columnas --
        df = df.rename(columns={
            'RUC': 'ruc', 'Periodo': 'periodo_tributario', 'CAR SUNAT': 'car_sunat',
            'Fecha de emisión': 'fecha_emision', 'Fecha Vcto/Pago': 'fecha_vencimiento',
            'Tipo CP/Doc.': 'tipo_comprobante', 'Serie del CDP': 'numero_serie',
            'Nro CP o Doc. Nro Inicial (Rango)': 'numero_correlativo',
            'Tipo Doc Identidad': 'tipo_documento', 'Nro Doc Identidad': 'numero_documento',
            'BI Gravado DG': 'bi_gravadas', 'IGV / IPM DG': 'igv_gravadas',
            'BI Gravado DGNG': 'bi_gravadas_nogravadas', 'IGV / IPM DGNG': 'igv_gravadas_nogravadas',
            'BI Gravado DNG': 'bi_nogravadas', 'IGV / IPM DNG': 'igv_nogravadas',
            'Valor Adq. NG': 'no_gravadas', 'ISC': 'isc', 'ICBPER': 'icbp',
            'Otros Trib/ Cargos': 'otros_cargos', 'Moneda': 'tipo_moneda',
            'Tipo CP Modificado': 'tipo_comprobante_modificado',
            'Serie CP Modificado': 'numero_serie_modificado',
            'Nro CP Modificado': 'numero_correlativo_modificado', 'Detracción': 'tasa_detraccion',
        })

        # -- Limpiar y Convertir Tipos de Datos --
        columnas_numericas = [
            'bi_gravadas', 'igv_gravadas', 'bi_gravadas_nogravadas', 'igv_gravadas_nogravadas',
            'bi_nogravadas', 'igv_nogravadas', 'no_gravadas', 'otros_cargos', 'isc', 'icbp'
        ]
        for col in columnas_numericas:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        columnas_fecha = ['fecha_emision', 'fecha_vencimiento']
        for col in columnas_fecha:
            df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d')

        df.loc[df['tasa_detraccion'] == 'D', 'tasa_detraccion'] = 0
        df['observaciones'] = ' SIRE:' + df['car_sunat']

        # -- Lógica de Negocio para 'destino' y consolidación de valores --
        condiciones = [
            ((df['bi_gravadas'] > 0) | (df['bi_gravadas_nogravadas'] > 0) | (df['bi_nogravadas'] > 0)) & (
                        df['no_gravadas'] > 0),
            (df['bi_gravadas'] > 0),
            (df['bi_gravadas_nogravadas'] > 0),
            (df['bi_nogravadas'] > 0),
            (df['no_gravadas'] > 0)
        ]
        resultados_destino = [5, 1, 2, 3, 4]

        df['destino'] = np.select(condiciones, resultados_destino, default=0)
        df['valor'] = (df['bi_gravadas'] + df['bi_gravadas_nogravadas'] + df['bi_nogravadas'])
        df['igv'] = (df['igv_gravadas'] + df['igv_gravadas_nogravadas'] + df['igv_nogravadas'])
        df.loc[df['destino'] == 4, 'valor'] = df['no_gravadas']
        df['otros'] = df['otros_cargos']
        df.loc[df['destino'] == 5, 'otros'] = df['otros_cargos'] + df['no_gravadas']

        # -- DataFrame Final --
        columnas_finales = [
            'ruc', 'periodo_tributario', 'observaciones', 'fecha_emision', 'fecha_vencimiento',
            'tipo_comprobante', 'numero_serie', 'numero_correlativo', 'tipo_documento',
            'numero_documento', 'isc', 'icbp', 'tipo_moneda', 'tipo_comprobante_modificado',
            'numero_serie_modificado', 'numero_correlativo_modificado', 'tasa_detraccion',
            'valor', 'igv', 'otros', 'destino'
        ]
        df_final = df[columnas_finales].copy()
        print("✅ Transformación de datos completada.")

        # --- 3. CARGAR ---
        engine = create_engine(db_url)
        print("🚀 Cargando datos en la base de datos...")
        df_final.to_sql(name=tabla_db, con=engine, schema=schema_db, if_exists='append', index=False)
        print(f"✅ ¡Carga de {len(df_final)} registros finalizada con éxito!")

        return df_final

    except Exception as e:
        print(f"❌ Ocurrió un error en el proceso ETL: {e}")
        return None


def generar_resumen_exploratorio(df: pd.DataFrame):
    """
    Muestra un resumen agrupado del DataFrame procesado.
    """
    if df is None or df.empty:
        print("No hay datos para generar un resumen.")
        return

    print("\n" + "=" * 50)
    print("--- RESUMEN EXPLORATORIO DE DATOS CARGADOS ---")
    print("=" * 50 + "\n")

    # Definir las agregaciones que queremos hacer
    agregaciones = {
        'valor': 'sum',
        'igv': 'sum',
        'icbp': 'sum',
        'otros': 'sum'
    }

    # Agrupar y calcular
    resumen = df.groupby(['tipo_comprobante', 'destino', 'tasa_detraccion', 'tipo_moneda']).agg(agregaciones).round(2)

    print(resumen.to_string())


# --- Ejemplo de Uso ---
if __name__ == "__main__":
    load_dotenv()

    # Parámetros del proceso
    ruta_del_zip = "C:/Users/Raknaros/Downloads/20614551373-20251006-174102-propuesta.zip"
    db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

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