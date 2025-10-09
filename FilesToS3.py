import os
import re
import msal
import requests
import boto3
import pandas as pd
from dotenv import load_dotenv
from io import BytesIO
from datetime import datetime

# Patrones regex para los nombres estructurados de archivos SUNAT
PATRONES_ESTRUCTURADOS = {
    "guia_remision": (re.compile(r"^(\d{11})-09-([A-Z0-9]{4})-(\d{1,8})\.(pdf|xml)$", re.IGNORECASE), ["ruc", "serie", "correlativo", "ext"]),
    "reporte_planilla_zip": (re.compile(r"^(\d{11})_[A-Z]+_(\d{8})\.(zip)$", re.IGNORECASE), ["ruc", "periodo", "ext"]),
    "declaraciones_pagos": (re.compile(r"^DetalleDeclaraciones_(\d{11})_(\d{14})\.(xlsx)$", re.IGNORECASE), ["ruc", "timestamp", "ext"]),
    "ficha_ruc": (re.compile(r"^reporteec_ficharuc_(\d{11})_(\d{14})\.(pdf)$", re.IGNORECASE), ["ruc", "timestamp", "ext"]),
    "ingreso_recaudacion": (re.compile(r"^ridetrac_(\d{11})_(\d{13})_(\d{14})_(\d{9})\.(pdf)$", re.IGNORECASE), ["ruc", "num_operacion", "timestamp", "id", "ext"]),
    "liberacion_fondos": (re.compile(r"^rilf_(\d{11})_(\d{13})_(\d{14})_(\d{9})\.(pdf)$", re.IGNORECASE), ["ruc", "num_operacion", "timestamp", "id", "ext"]),
    "multa": (re.compile(r"^rmgen_(\d{11})_(\d{3})-(\d{3})-(\d{7})_(\d{14})_(\d{9})\.(pdf)$", re.IGNORECASE), ["ruc", "cod1", "cod2", "num_multa", "timestamp", "id", "ext"]),
    "notificacion": (re.compile(r"^constancia_(\d{14})_(\d{20})_(\d{13})_(\d{9})\.(pdf)$", re.IGNORECASE), ["timestamp", "id_notif", "num_operacion", "id", "ext"]),
    "valores": (re.compile(r"^rvalores_(\d{11})_([A-Z0-9]{12,17})_(\d{14})_(\d{9})\.(pdf)$", re.IGNORECASE), ["ruc", "num_valor", "timestamp", "id", "ext"]),
    "coactiva": (re.compile(r"^recgen_(\d{11})_(\d{13})_(\d{14})_(\d{9})\.(pdf)$", re.IGNORECASE), ["ruc", "num_expediente", "timestamp", "id", "ext"]),
    "baja_oficio": (re.compile(r"^bod_(\d{6})_(\d{11})_(\d{4})\.(pdf)$", re.IGNORECASE), ["id_baja", "ruc", "periodo", "ext"]),
    "factura": (re.compile(r"^(?:\d{11}-)?(01)-([A-Z0-9]{4})-(\d{1,8})\.(xml|zip|pdf)$", re.IGNORECASE), ["tipo_doc", "serie", "correlativo", "ext"]),
    "boleta": (re.compile(r"^(?:\d{11}-)?(03)-([A-Z0-9]{4})-(\d{1,8})\.(xml|zip|pdf)$", re.IGNORECASE), ["tipo_doc", "serie", "correlativo", "ext"]),
    "nota_credito": (re.compile(r"^(?:\d{11}-)?(07)-([A-Z0-9]{4})-(\d{1,8})\.(xml|zip|pdf)$", re.IGNORECASE), ["tipo_doc", "serie", "correlativo", "ext"]),
    "nota_debito": (re.compile(r"^(?:\d{11}-)?(08)-([A-Z0-9]{4})-(\d{1,8})\.(xml|zip|pdf)$", re.IGNORECASE), ["tipo_doc", "serie", "correlativo", "ext"]),
    "recibo_honorarios": (re.compile(r"^(?:\d{11}-)?(RHE)-([A-Z0-9]{4})-(\d{1,8})\.(xml|pdf)$", re.IGNORECASE), ["tipo_doc", "serie", "correlativo", "ext"]),
    "pdf_doc": (re.compile(r"^PDF-DOC-([A-Z0-9]{4})(\d{1,8})(\d{11})\.(pdf|xml|zip)$", re.IGNORECASE), ["serie", "correlativo", "ruc", "ext"]),
}



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
    refresh_token = os.getenv('MS_REFRESH_TOKEN')
    ms_config = {
        "client_id": os.getenv('ONEDRIVE_CLIENT_ID') or os.getenv('MS_CLIENT_ID'),
        "client_secret": os.getenv('MS_CLIENT_SECRET'),
        "refresh_token": refresh_token,
        "authority": "https://login.microsoftonline.com/common",
        "scopes": ['Files.ReadWrite.All'],
        "use_refresh": bool(refresh_token)  # True si hay refresh token
    }

    return s3_client, s3_bucket, ms_config


def get_graph_token(config):
    """Obtiene un access token de Microsoft Graph."""
    if config.get("use_refresh"):
        # Usar refresh token
        app = msal.ConfidentialClientApplication(
            client_id=config["client_id"],
            authority=config["authority"],
            client_credential=config["client_secret"]
        )
        result = app.acquire_token_by_refresh_token(config["refresh_token"], scopes=config["scopes"])
    else:
        # Usar device flow
        app = msal.PublicClientApplication(
            client_id=config["client_id"],
            authority=config["authority"]
        )
        flow = app.initiate_device_flow(scopes=config["scopes"])
        if "user_code" not in flow:
            raise ValueError("Fallo al crear el flujo de dispositivo.", flow.get("error_description"))

        print(flow["message"])
        result = app.acquire_token_by_device_flow(flow)
        # Imprimir el refresh token para configuración en .env
        if "access_token" in result and "refresh_token" in result:
            print(f"\n--- REFRESH TOKEN PARA .ENV ---\nMS_REFRESH_TOKEN={result['refresh_token']}\n-------------------------------\n")

    if "access_token" in result:
        return result["access_token"]
    else:
        raise Exception(f"No se pudo obtener el access token: {result.get('error_description')}")


def listar_archivos_onedrive_recursivo(token, folder_path="AbacoBot"):
    """
    Lista RECURSIVAMENTE todos los archivos dentro de una carpeta específica
    en OneDrive usando el endpoint de children.
    """
    headers = {'Authorization': 'Bearer ' + token}
    archivos_totales = []

    def recurse(current_path):
        endpoint = f"https://graph.microsoft.com/v1.0/me/drive/root:/{current_path}:/children"
        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            data = response.json()

            for item in data.get('value', []):
                if 'folder' in item:
                    # Es una carpeta, recursar
                    sub_path = f"{current_path}/{item['name']}"
                    recurse(sub_path)
                else:
                    # Es un archivo, añadir a la lista
                    archivos_totales.append(item)

        except requests.exceptions.RequestException as e:
            print(f"❌ Error al listar {current_path}: {e}")
            if e.response:
                print(f"Detalles: {e.response.json()}")

    print(f"Buscando todos los archivos dentro de la carpeta '{folder_path}' en OneDrive...")
    recurse(folder_path)
    return archivos_totales


# --- 2. LÓGICA DE PROCESAMIENTO ---

def extraer_ruc_de_nombre(nombre_archivo: str) -> str | None:
    """Extrae el RUC usando patrones estructurados de SUNAT."""
    for patron, grupos in PATRONES_ESTRUCTURADOS.values():
        match = patron.match(nombre_archivo)
        if match:
            # Encontrar el índice de "ruc" en la lista de grupos
            if "ruc" in grupos:
                idx = grupos.index("ruc") + 1  # Grupos empiezan en 1
                ruc = match.group(idx)
                # Validar que sea 11 dígitos y empiece con 10, 15 o 20
                if re.match(r'^(10|15|20)\d{9}$', ruc):
                    return ruc
    return None


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
        print("Iniciando configuración del entorno...")
        s3_client, s3_bucket, ms_config = configurar_entorno()
        print("Configuración del entorno exitosa.")

        print("Obteniendo token de Microsoft Graph...")
        ms_token = get_graph_token(ms_config)
        print("Token obtenido exitosamente.")

        print("Listando archivos en OneDrive...")
        archivos_onedrive = listar_archivos_onedrive_recursivo(ms_token)
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

            print(f"Verificando existencia en S3 para {s3_key}...")
            if verificar_si_archivo_existe_s3(s3_client, s3_bucket, s3_key):
                reporte.append({'archivo': nombre, 'ruc_encontrado': ruc, 'estado': 'Duplicado en S3'})
                print(f"Archivo {nombre} ya existe en S3.")
            else:
                try:
                    download_url = archivo_info.get('@microsoft.graph.downloadUrl')
                    if not download_url:
                        raise Exception("No se encontró URL de descarga.")

                    print(f"Copiando {nombre} de OneDrive a S3...")
                    copiar_archivo_de_onedrive_a_s3(s3_client, s3_bucket, s3_key, download_url)
                    reporte.append({'archivo': nombre, 'ruc_encontrado': ruc, 'estado': 'Copiado a S3'})
                    print(f"Archivo {nombre} copiado exitosamente.")
                except Exception as e:
                    reporte.append({'archivo': nombre, 'ruc_encontrado': ruc, 'estado': f'Error al copiar: {e}'})
                    print(f"Error al copiar {nombre}: {e}")

    except Exception as e:
        print(f"❌ Ocurrió un error general: {e}")
        import traceback
        traceback.print_exc()
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