import pdfkit
import base64
import os
from jinja2 import Environment, FileSystemLoader


def image_to_base64(path):
    if not os.path.exists(path): return ""
    with open(path, "rb") as f:
        return f"data:image/png;base64,{base64.b64encode(f.read()).decode('utf-8')}"


# Configuración de Identidad Visual por Emisor
CONFIG_EMISORES = {
    "THB_ELECTRIC": {
        "template_name": "proforma_thb.html",
        "logo_path": r"D:\pytest\images\logo_tbhelectric.png",
        "estilo": {
            "color_primario": "#002E6D",
            "fuente": "'Helvetica', sans-serif",
            "textura_fondo": ""
        }
    },
    "LIBELAW": {
        "template_name": "factura_libelaw.html",
        "logo_path": r"D:\pytest\images\logo_libelaw.png",
        "estilo": {
            "color_primario": "#E31C24",
            "fuente": "'Verdana', sans-serif",
            "textura_fondo": "https://www.transparenttextures.com/patterns/carbon-fibre.png"
        }
    }
}

env = Environment(loader=FileSystemLoader('templates'))
config_pdf = pdfkit.configuration(wkhtmltopdf=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe')
options = {'page-size': 'A4', 'encoding': "UTF-8", 'no-outline': None, 'enable-local-file-access': None}


def procesar_documentos(df_cabecera, df_detalle):
    for _, fila in df_cabecera.iterrows():
        proveedor_key = fila['alias_proveedor']
        conf = CONFIG_EMISORES.get(proveedor_key)

        if not conf:
            print(f"⚠️ Sin configuración para {proveedor_key}");
            continue

        # Los datos bancarios y montos ya vienen en la 'fila' del df_cabecera
        contexto = fila.to_dict()

        # Filtramos los ítems del detalle
        servicios_items = df_detalle[df_detalle['cui'] == fila['cui']].to_dict('records')

        # Unimos todo en el contexto para Jinja2
        contexto.update({
            "logo_base64": image_to_base64(conf['logo_path']),
            "servicios": servicios_items,
            "estilo": conf['estilo'],
            "total": fila.get('total', 0)  # Asegúrate que tu DF tenga la columna total
        })

        template = env.get_template(conf['template_name'])
        html_out = template.render(contexto)

        if not os.path.exists("salida"): os.makedirs("salida")
        nombre_salida = f"salida/{fila['cui']}_{fila['cliente'][:15]}.pdf"

        try:
            pdfkit.from_string(html_out, nombre_salida, configuration=config_pdf, options=options)
            print(f"✅ Generado: {nombre_salida}")
        except Exception as e:
            print(f"❌ Error en {fila['cui']}: {e}")


# --- SCRIPT DE PREVISUALIZACIÓN ---
def previsualizar_plantilla(nombre_plantilla, datos_prueba):
    template = env.get_template(nombre_plantilla)
    html_renderizado = template.render(datos_prueba)

    # Guardamos el resultado en un archivo que Chrome SÍ puede leer
    with open("preview_final.html", "w", encoding="utf-8") as f:
        f.write(html_renderizado)

    print("✅ Previsualización lista. Abre 'preview_final.html' en tu navegador.")


# Datos de ejemplo para que no salga vacío
datos_test = {
    "estilo": CONFIG_EMISORES["LIBELAW"]["estilo"],
    "logo_base64": image_to_base64(CONFIG_EMISORES["LIBELAW"]["logo_path"]),
    "nro_cotizacion": "TEST-001",
    "fecha": "20/01/2026",
    "cliente": "CLIENTE DE PRUEBA S.A.C.",
    "ruc": "20123456789",
    "servicios": [{"descripcion": "Servicio de prueba", "monto": 100.50}],
    "total": 100.50,
    "banco_nombre": "BCP",
    "banco_cuenta": "191-000-000",
    "banco_cci": "002-191-000",
    "banco_titular": "LIBELAW"
}

previsualizar_plantilla("factura_impulsa.html", datos_test)
# Ejemplo de uso:
# procesar_documentos(df_cabecera, df_detalle)