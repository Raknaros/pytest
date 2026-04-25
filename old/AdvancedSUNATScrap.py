import requests
from bs4 import BeautifulSoup

# (Tu código de session.headers.update se mantiene)
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    # ... (otros headers que quieras añadir)
})

# --- URLs Clave (las que descubriste) ---
URL_LOGIN_PAGE = "https://api-seguridad.sunat.gob.pe/v1/clientessol/4f3b88b3-d9d6-402a-b85d-6a0bc857746a/oauth2/loginMenuSol?lang=es-PE&showDni=true&showLanguages=false&originalUrl=https://e-menu.sunat.gob.pe/cl-ti-itmenu/AutenticaMenuInternet.htm&state=rO0ABXNyABFqYXZhLnV0aWwuSGFzaE1hcAUH2sHDFmDRAwACRgAKbG9hZEZhY3RvckkACXRocmVzaG9sZHhwP0AAAAAAAAx3CAAAABAAAAADdAAEZXhlY3B0AAZwYXJhbXN0AEsqJiomL2NsLXRpLWl0bWVudS9NZW51SW50ZXJuZXQuaHRtJmI2NGQyNmE4YjVhZjA5MTkyM2IyM2I2NDA3YTFjMWRiNDFlNzMzYTZ0AANleGVweA=="
URL_SECURITY_CHECK = "https://api-seguridad.sunat.gob.pe/v1/clientessol/4f3b88b3-d9d6-402a-b85d-6a0bc857746a/oauth2/j_security_check"

# --- Credenciales ---
RUC = "20606283858"
USER_SOL = "TONERTAT"
PASS_SOL = "rcavinsio"  # <-- PUSISTE LA CLAVE INCORRECTA A PROPÓSITO

try:
    print(f"Iniciando conexión a la página de login...")
    response_login_page = session.get(URL_LOGIN_PAGE)
    response_login_page.raise_for_status()
    print("Conexión inicial exitosa. Obteniendo cookies de sesión.")

    # --- Payload del Login ---
    login_payload = {
        'tipo': '2',
        'dni': '',
        'custom_ruc': RUC,
        'j_username': USER_SOL,
        'j_password': PASS_SOL,  # <-- La clave incorrecta
        'captcha': '',
        'originalUrl': 'https://e-menu.sunat.gob.pe/cl-ti-itmenu/AutenticaMenuInternet.htm',
        'lang': 'es-PE',
        'state': 'rO0ABXNyABFqYXZhLnV0aWwuSGFzaE1hcAUH2sHDFmDRAwACRgAKbG9hZEZhY3RvckkACXRocmVzaG9sZHhwP0AAAAAAAAx3CAAAABAAAAADdAAEZXhlY3B0AAZwYXJhbXN0AEsqJiomL2NsLXRpLWl0bWVudS9NZW51SW50ZXJuZXQuaHRtJmI2NGQyNmE4YjVhZjA5MTkyM2IyM2I2NDA3YTFjMWRiNDFlNzMzYTZ0AANleGVweA=='
    }

    headers_post = {
        "Origin": "https://api-seguridad.sunat.gob.pe",
        "Referer": URL_LOGIN_PAGE,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    print("Enviando credenciales a j_security_check...")

    # --- LÓGICA DE VERIFICACIÓN CORREGIDA ---
    # allow_redirects=False nos permite inspeccionar la cabecera 'Location'
    response_post_login = session.post(URL_SECURITY_CHECK, data=login_payload, headers=headers_post,
                                       allow_redirects=False)

    # 1. VERIFICAR SI EL LOGIN FUE EXITOSO
    # El éxito es un código 302 Y que la URL de redirección contenga "code="
    if response_post_login.status_code == 302 and 'code=' in response_post_login.headers.get('Location', ''):

        print("✅ ¡Login POST exitoso! Redireccionando para canjear el código...")

        # Ahora sí, podemos seguir las redirecciones
        url_redirect_1 = response_post_login.headers['Location']

        # Hacemos el GET a la siguiente URL.
        # La librería 'requests' es lo bastante inteligente para que
        # 'session' siga automáticamente las redirecciones subsiguientes.
        response_menu = session.get(url_redirect_1)

        # La prueba final es si la página final cargó Y si NO es la página de login
        if response_menu.status_code == 200 and "j_security_check" not in response_menu.text:
            print("✅ ¡Login completo! Sesión activa.")
            # Aquí es donde llamarías a la función de descarga
            # ej: descargar_xml(session, 'E001-349')
        else:
            print("❌ Error: El canje de código falló o la página de menú no cargó.")

    # 2. VERIFICAR SI EL LOGIN FALLÓ (Contraseña incorrecta o CAPTCHA)
    else:
        print("❌ El login falló. El servidor no devolvió un código de autorización.")

        if response_post_login.status_code == 200:
            print("  Razón: El servidor devolvió 200 OK. (Probablemente credenciales incorrectas o CAPTCHA requerido).")
            # print(response_post_login.text) # Descomenta para ver el HTML de la página de error

        elif response_post_login.status_code == 302:
            print("  Razón: El servidor redirigió (302) pero la URL no contenía un 'code='.")
            print(f"  Redirigido a: {response_post_login.headers.get('Location')}")

        else:
            print(f"  Razón: Código de estado inesperado: {response_post_login.status_code}")

except requests.exceptions.RequestException as e:
    print(f"❌ Ocurrió un error de red: {e}")