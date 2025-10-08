import os
import re
import msal
import requests
import boto3
import pandas as pd
from dotenv import load_dotenv
from io import BytesIO
from datetime import datetime



# --- 1. CONFIGURACIÓN Y CLIENTES ---
def configurar_entorno():
    """Carga variables de entorno y configura los clientes de API."""
    load_dotenv()

    # Configuración de S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    s3_bucket = os.getenv('AWS_S3_BUCKET_NAME')

    # Configuración de Microsoft
    ms_config = {
        "client_id": os.getenv('MS_CLIENT_ID'),
        "client_secret": os.getenv('MS_CLIENT_SECRET'),
        "refresh_token": os.getenv('MS_REFRESH_TOKEN'),
        "authority": "https://login.microsoftonline.com/common",
        "scopes": ['Files.ReadWrite.All']
    }

    return s3_client, s3_bucket, ms_config


def get_graph_token(config):
    """Obtiene un access token de Microsoft Graph usando el refresh token."""
    app = msal.ConfidentialClientApplication(
        client_id=config["client_id"],
        authority=config["authority"],
        client_credential=config["client_secret"]
    )
    result = app.acquire_token_by_refresh_token(config["refresh_token"], scopes=config["scopes"])
    if "access_token" in result:
        return result["access_token"]
    else:
        raise Exception(f"No se pudo obtener el access token: {result.get('error_description')}")


def listar_archivos_onedrive(token, carpeta_raiz="AbacoBot"):
    """
    Busca RECURSIVAMENTE todos los archivos dentro de una carpeta específica
    en OneDrive.
    """
    # El endpoint de búsqueda. Filtramos por la ruta de la carpeta raíz.
    # El : al final de la ruta es importante.
    endpoint = f"https://graph.microsoft.com/v1.0/me/drive/root/search(q='')?$select=name,id,@microsoft.graph.downloadUrl&filter=startsWith(parentReference/path, '/drive/root:/{carpeta_raiz}')"

    headers = {'Authorization': 'Bearer ' + token}
    archivos_totales = []

    print(f"Buscando todos los archivos dentro de la carpeta '{carpeta_raiz}' en OneDrive...")

    while endpoint:
        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            data = response.json()

            # Añadimos los archivos de esta "página" a nuestra lista total
            archivos_totales.extend(data.get('value', []))

            # Microsoft Graph pagina los resultados. Si hay más archivos,
            # nos dará una URL para la siguiente página.
            endpoint = data.get('@odata.nextLink')

        except requests.exceptions.RequestException as e:
            print(f"❌ Error al llamar a la API de Graph: {e}")
            if e.response:
                print(f"Detalles del error: {e.response.json()}")
            return []  # Devolvemos una lista vacía en caso de error

    return archivos_totales


# --- 2. LÓGICA DE PROCESAMIENTO ---

def extraer_ruc_de_nombre(nombre_archivo: str) -> str | None:
    """Busca un RUC de 11 dígitos que empiece con 10, 15 o 20 en un string."""
    # Expresión regular para encontrar el RUC
    match = re.search(r'\b(10|15|20)\d{9}\b', nombre_archivo)
    return match.group(0) if match else None


def verificar_si_archivo_existe_s3(s3_client, bucket, key):
    """
    Verifica si un objeto existe en S3 usando list_objects_v2.
    Es una alternativa robusta a head_object.
    """
    try:
        # Pide una lista de objetos que coincidan exactamente con la clave (key)
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)

        # Si 'Contents' existe en la respuesta y el nombre de la clave es exacto,
        # significa que el archivo fue encontrado.
        if 'Contents' in response and response['Contents'][0]['Key'] == key:
            return True
        return False
    except s3_client.exceptions.ClientError as e:
        # Manejar otros posibles errores, como acceso denegado a ListBucket
        print(f"Error de cliente de AWS al verificar el archivo: {e}")
        return False

def copiar_archivo_de_onedrive_a_s3(s3_client, bucket, key, onedrive_download_url):
    """Descarga un archivo de OneDrive y lo sube a S3 en streaming."""
    with requests.get(onedrive_download_url, stream=True) as r:
        r.raise_for_status()
        s3_client.upload_fileobj(r.raw, bucket, key)


# --- 3. ORQUESTADOR PRINCIPAL ---

def procesar_archivos():
    """Función principal que orquesta todo el proceso."""
    reporte = []

    try:
        s3_client, s3_bucket, ms_config = configurar_entorno()
        ms_token = get_graph_token(ms_config)
        archivos_onedrive = listar_archivos_onedrive(ms_token)

        print(f"Se encontraron {len(archivos_onedrive)} archivos en total en OneDrive.")

        # --- CAMBIO 1: Filtrar primero los archivos que contienen un RUC ---
        archivos_con_ruc = []
        for archivo in archivos_onedrive:
            nombre = archivo.get('name')
            if not nombre:
                continue
            ruc = extraer_ruc_de_nombre(nombre)
            if ruc:
                # Guardamos el archivo y el ruc encontrado para el siguiente paso
                archivos_con_ruc.append({'archivo_info': archivo, 'ruc': ruc})
            else:
                reporte.append({'archivo': nombre, 'ruc_encontrado': 'No', 'estado': 'Ignorado (sin RUC)'})

        print(f"Se procesarán {len(archivos_con_ruc)} archivos que contienen RUC.")

        # --- CAMBIO 2: Procesar la lista ya filtrada ---
        for item in archivos_con_ruc:
            archivo_info = item['archivo_info']
            ruc = item['ruc']
            nombre = archivo_info['name']

            # La 'key' en S3 es la ruta completa: RUC/nombre_de_archivo.ext
            s3_key = f"{ruc}/{nombre}"

            if verificar_si_archivo_existe_s3(s3_client, s3_bucket, s3_key):
                reporte.append({'archivo': nombre, 'ruc_encontrado': ruc, 'estado': 'Duplicado en S3'})
            else:
                try:
                    download_url = archivo_info.get('@microsoft.graph.downloadUrl')
                    if not download_url:
                        raise Exception("No se encontró URL de descarga.")

                    copiar_archivo_de_onedrive_a_s3(s3_client, s3_bucket, s3_key, download_url)
                    reporte.append({'archivo': nombre, 'ruc_encontrado': ruc, 'estado': 'Copiado a S3'})
                except Exception as e:
                    reporte.append({'archivo': nombre, 'ruc_encontrado': ruc, 'estado': f'Error al copiar: {e}'})

    except Exception as e:
        print(f"❌ Ocurrió un error general: {e}")
        return

    # --- CAMBIO 3: Generar reporte en TXT con timestamp ---
    if reporte:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        nombre_log = f"file_process_{timestamp}.txt"

        with open(nombre_log, "w", encoding="utf-8") as f:
            f.write("--- Reporte de Procesamiento de Archivos ---\n")
            f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 40 + "\n")
            for entrada in reporte:
                f.write(f"Archivo: {entrada['archivo']}\n")
                f.write(f"  - RUC: {entrada['ruc_encontrado']}\n")
                f.write(f"  - Estado: {entrada['estado']}\n")
                f.write("-" * 20 + "\n")

        print(f"\n✅ Reporte de procesamiento guardado en '{nombre_log}'")


if __name__ == "__main__":
    # (Pega aquí las definiciones de las funciones que faltan: configurar_entorno, get_graph_token, etc.)
    procesar_archivos()