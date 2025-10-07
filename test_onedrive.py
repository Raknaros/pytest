import json
import os
import msal
import requests
from dotenv import load_dotenv

# --- 1. CONFIGURACIÓN ---
load_dotenv()

# Copia y pega tus credenciales desde el portal de Microsoft Entra
# Puedes ponerlas en tu .env o directamente aquí para esta prueba
CLIENT_ID = os.getenv('ONEDRIVE_CLIENT_ID') or "TU_ID_DE_APLICACION_CLIENTE"
TENANT_ID = os.getenv('ONEDRIVE_TENANT_ID') or "TU_ID_DE_DIRECTORIO_INQUILINO"

# El "endpoint" de la API de Microsoft Graph para listar archivos en la raíz de tu OneDrive
GRAPH_ENDPOINT = 'https://graph.microsoft.com/v1.0/me/drive/root/children'

# Los "scopes" son los permisos que tu script solicitará
SCOPES = ['Files.ReadWrite.All']  # Debe coincidir con los permisos que configuraste

# Autoridad de autenticación
AUTHORITY = f"https://login.microsoftonline.com/common"

# --- 2. AUTENTICACIÓN ---
# Inicializa el cliente de MSAL
app = msal.PublicClientApplication(
    CLIENT_ID,
    authority=AUTHORITY
)

# Inicia el flujo de código de dispositivo
flow = app.initiate_device_flow(scopes=SCOPES)

if "user_code" not in flow:
    raise ValueError("Fallo al crear el flujo de dispositivo. Revisa tus credenciales.", flow.get("error_description"))

# Muestra al usuario las instrucciones para autenticarse
print(flow["message"])

# Espera a que el usuario se autentique en el navegador
result = app.acquire_token_by_device_flow(flow)

# --- 3. LLAMADA A LA API ---
if "access_token" in result:
    print("\n✅ ¡Autenticación exitosa!")

    # --- AÑADE ESTA LÍNEA PARA DEPURAR ---
    print(
        f"\n--- TU TOKEN DE ACCESO (COPIA TODO LO QUE SIGUE) ---\n{result['access_token']}\n--------------------------------------------\n")

    # Prepara las cabeceras con el token de acceso
    headers = {'Authorization': 'Bearer ' + result['access_token']}
    try:
        print("🚀 Realizando llamada a la API de Microsoft Graph para listar archivos...")
        # Realiza la petición GET a la API
        response = requests.get(GRAPH_ENDPOINT, headers=headers)
        response.raise_for_status()  # Lanza un error si la respuesta no es 2xx

        # Imprime la respuesta de la API de forma legible
        files_data = response.json()
        print("\n--- Archivos en la raíz de tu OneDrive ---")
        for item in files_data.get('value', []):
            print(f"- {item['name']} (Tipo: {item.get('folder', {}).get('childCount', 'Archivo')})")

    except requests.exceptions.RequestException as e:
        print(f"❌ Error al llamar a la API de Graph: {e}")
        # Si hay un error, a menudo el cuerpo de la respuesta tiene más detalles
        if e.response:
            print(f"Detalles del error: {e.response.json()}")

else:
    print(f"❌ Error al adquirir el token: {result.get('error')}")
    print(result.get('error_description'))
