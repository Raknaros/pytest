import requests
import json

# 1. Definir la URL y los Headers
url = "https://db-api.giumarchan.com/api/acc/declaraciones?periodo=202602&ruc=20606283858"
headers = {
    "X-API-Key": "Giu12FF8DB*"
}

try:
    # 2. Hacer la petición GET
    response = requests.get(url, headers=headers)

    # 3. Lanzar una excepción si el servidor responde con error (ej. 401, 500)
    response.raise_for_status()

    # 4. Parsear la respuesta a JSON y mostrarla bonita
    data = response.json()
    print("=== Respuesta Exitosa ===")
    print(json.dumps(data, indent=4))

except requests.exceptions.HTTPError as err:
    print(f"Error HTTP: {err}")
    print(f"Detalle: {response.text}")
except Exception as e:
    print(f"Error de conexión: {e}")