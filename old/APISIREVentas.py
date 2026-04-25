import os
import requests
from dotenv import load_dotenv
import sys
import pandas as pd

from sqlalchemy import create_engine

# --- 1. Cargar Configuración ---
load_dotenv()
CLIENT_ID = os.getenv('SUNAT_CLIENT_ID')
CLIENT_SECRET = os.getenv('SUNAT_CLIENT_SECRET')
RUC = os.getenv('SUNAT_RUC')
SOL_USER = os.getenv('SUNAT_SOL_USER')
SOL_PASS = os.getenv('SUNAT_SOL_PASS')
# --- Configuración de Servicios ---
RUN_EXPORT_PROPOSAL = True
RUN_QUERY_STATUS = False
RUN_DOWNLOAD_FILE = False

# Parámetros para descarga de archivo (rellenar según resultado de consulta de estado)
NOM_ARCHIVO_REPORTE = "LE2060628385820251100014040001EXP2.zip"  # Ej. "reporte_202509.zip"
COD_TIPO_ARCHIVO_REPORTE = "00"  # Ej. "01"
COD_LIBRO = "140000"  # Ej. "1409"
# Conexión a Warehouse (Contabilidad)
warehouse_url = f"{os.getenv('DB_WAREHOUSE_DIALECT')}://{os.getenv('DB_WAREHOUSE_USER')}:{os.getenv('DB_WAREHOUSE_PASSWORD')}@{os.getenv('DB_WAREHOUSE_HOST')}:{os.getenv('DB_WAREHOUSE_PORT')}/{os.getenv('DB_WAREHOUSE_NAME')}"
warehouse = create_engine(warehouse_url)

#credenciales=pd.read_sql(
#    "SELECT entities.ruc, entities.usuario_sol, entities.clave_sol otras_credenciales.usuario, otras_credenciales.contrasena FROM priv.entities JOIN priv.otras_credenciales ON entities.ruc=otras_credenciales.ruc", con=warehouse)

# --- URLs de SUNAT (Basadas en tu información) ---
# Formateamos la URL de token con el client_id
TOKEN_URL = f"https://api-seguridad.sunat.gob.pe/v1/clientessol/{CLIENT_ID}/oauth2/token/"
# URL de ejemplo del API SIRE
SIRE_API_URL = "https://api-sire.sunat.gob.pe/v1/contribuyente/migeigv/libros/rvie/propuesta/web/propuesta"


def get_sunat_access_token() -> str | None:
    """
    Se autentica contra SUNAT para obtener un Bearer Token.
    Usa el flujo 'password' (Clave SOL).
    """
    print(f"Iniciando autenticación para Client ID: {CLIENT_ID}...")

    # 1. Construir el username (RUC + Usuario SOL)
    username_sol = f"{RUC}{SOL_USER}"

    # 2. Definir el payload (cuerpo) de la petición
    payload = {
        'grant_type': 'password',
        'scope': 'https://api-cpe.sunat.gob.pe',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'username': username_sol,
        'password': SOL_PASS
    }

    # 3. Definir las cabeceras (headers)
    # Como el body es 'x-www-form-urlencoded', usamos el parámetro 'data' en requests.
    # 'requests' pondrá el 'Content-Type' correcto automáticamente.
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    try:
        # 4. Hacemos el POST a la URL de tokens
        response = requests.post(TOKEN_URL, data=payload, headers=headers, timeout=10)

        # Verificar si la petición fue exitosa
        response.raise_for_status()

        data = response.json()
        access_token = data.get('access_token')

        if not access_token:
            print("❌ Error: La respuesta de autenticación no incluyó un 'access_token'.")
            print(data)
            return None

        print("✅ Token de acceso obtenido exitosamente.")
        return access_token

    except requests.exceptions.HTTPError as http_err:
        print(f"❌ Error HTTP al obtener token: {http_err}")
        print(f"Respuesta del servidor: {http_err.response.text}")
    except Exception as e:
        print(f"❌ Ocurrió un error inesperado al obtener token: {e}")

    return None


def exportar_propuesta_sire(token: str, per_tributario: str):
    """
    Realiza la consulta GET de ejemplo al endpoint de SIRE.
    """
    # 1. Construir la URL completa del endpoint
    url_completa = f"{SIRE_API_URL}/{per_tributario}/exportapropuesta?codTipoArchivo=0"
    print(f"Realizando consulta a: {url_completa}")

    # 2. Preparar la cabecera de autorización
    headers = {
        'Authorization': f'Bearer {token}'
    }

    try:
        response = requests.get(url_completa, headers=headers, timeout=10)
        response.raise_for_status()

        # 3. Si todo sale bien, muestra los datos
        data = response.json()
        print("✅ Consulta exitosa:")
        print(data)
        # Deberías ver algo como: {'numTicket': '...'}
        return data

    except requests.exceptions.HTTPError as http_err:
        print(f"❌ Error HTTP al consultar API: {http_err}")


        print(f"Respuesta del servidor: {http_err.response.text}")
        return None
    except Exception as e:
        print(f"❌ Ocurrió un error inesperado al consultar API: {e}")
        return None
def consulta_estado_ticket(token: str, perIni: str, perFin: str, page: int, perPage: int, numTicket: str):
    """
    Consulta el estado de un ticket en el API de SIRE.
    """
    # 1. Construir la URL completa del endpoint
    url_completa = f"https://api-sire.sunat.gob.pe/v1/contribuyente/migeigv/libros/rvierce/gestionprocesosmasivos/web/masivo/consultaestadotickets?perIni={perIni}&perFin={perFin}&page={page}&perPage={perPage}&numTicket={numTicket}"
    print(f"Consultando estado de ticket: {url_completa}")

    # 2. Preparar la cabecera de autorización
    headers = {
        'Authorization': f'Bearer {token}'
    }

    try:
        response = requests.get(url_completa, headers=headers, timeout=10)
        response.raise_for_status()

        # 3. Si todo sale bien, muestra los datos
        data = response.json()
        print("✅ Consulta de estado de ticket exitosa:")
        print(data)
        return data

    except requests.exceptions.HTTPError as http_err:
        print(f"❌ Error HTTP al consultar estado de ticket: {http_err}")
        print(f"Respuesta del servidor: {http_err.response.text}")
    except Exception as e:
        print(f"❌ Ocurrió un error inesperado al consultar estado de ticket: {e}")

    return None

def descargar_archivo(token: str, nomArchivoReporte: str, codTipoArchivoReporte: str, codLibro: str, perTributario: str, codProceso: str, numTicket: str):
    """
    Descarga un archivo de reporte desde el API de SIRE.
    """
    # 1. Construir la URL completa del endpoint
    url_completa = f"https://api-sire.sunat.gob.pe/v1/contribuyente/migeigv/libros/rvierce/gestionprocesosmasivos/web/masivo/archivoreporte?nomArchivoReporte={nomArchivoReporte}&codTipoArchivoReporte={codTipoArchivoReporte}&codLibro={codLibro}&perTributario={perTributario}&codProceso={codProceso}&numTicket={numTicket}"
    print(f"Descargando archivo: {url_completa}")

    # 2. Preparar la cabecera de autorización
    headers = {
        'Authorization': f'Bearer {token}'
    }

    try:
        response = requests.get(url_completa, headers=headers, timeout=10)
        response.raise_for_status()

        # 3. Guardar el archivo descargado
        with open(nomArchivoReporte, 'wb') as f:
            f.write(response.content)
        print(f"✅ Archivo descargado exitosamente: {nomArchivoReporte}")

    except requests.exceptions.HTTPError as http_err:
        print(f"❌ Error HTTP al descargar archivo: {http_err}")
        print(f"Respuesta del servidor: {http_err.response.text}")
    except Exception as e:
        print(f"❌ Ocurrió un error inesperado al descargar archivo: {e}")





# --- Flujo Principal de Ejecución ---
if __name__ == "__main__":
    if not all([os.getenv('SUNAT_CLIENT_ID'), os.getenv('SUNAT_CLIENT_SECRET'), os.getenv('SUNAT_RUC'), os.getenv('SUNAT_SOL_USER'), os.getenv('SUNAT_SOL_PASS')]):
        print("Error fatal: Faltan una o más variables de entorno (.env).")
        sys.exit(1)

    token = get_sunat_access_token()
    if not token:
        print("No se pudo obtener el token de acceso.")
        sys.exit(1)

    per_tributario = "202509"  # Período tributario a consultar
    numTicket = 20250300000021

    # Ejecutar servicios según flags
    if RUN_EXPORT_PROPOSAL:
        print("Ejecutando: Exportar Propuesta")
        data = exportar_propuesta_sire(token, per_tributario)
        if data and isinstance(data, dict) and 'numTicket' in data:
            numTicket = data['numTicket']
            print(f"NumTicket obtenido: {numTicket}")
        else:
            print("No se pudo obtener numTicket de la respuesta de la propuesta.")

    if RUN_QUERY_STATUS and numTicket:
        print("Ejecutando: Consulta Estado de Ticket")
        consulta_estado_ticket(token, per_tributario, per_tributario, 1, 20, numTicket)

    if RUN_DOWNLOAD_FILE:
        print("Ejecutando: Descargar Archivo")
        if NOM_ARCHIVO_REPORTE and COD_TIPO_ARCHIVO_REPORTE and COD_LIBRO and numTicket:
            descargar_archivo(token, NOM_ARCHIVO_REPORTE, COD_TIPO_ARCHIVO_REPORTE, COD_LIBRO, per_tributario, "10", numTicket)
        else:
            print("Parámetros de descarga no configurados o numTicket no disponible.")
